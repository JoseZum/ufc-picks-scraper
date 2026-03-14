import scrapy
import re
from datetime import datetime, date


class UfcSpider(scrapy.Spider):
    name = "ufc"
    allowed_domains = ["tapology.com"]

    # Fecha minima para scraping - eventos antes de esta fecha se ignoran
    # Esto evita scraping infinito de eventos historicos
    MIN_DATE = date(2026, 1, 1)

    def __init__(self, EVENT_ID=None, MODE=None, SKIP_BOUT_DETAILS=None, *args, **kwargs):
        super(UfcSpider, self).__init__(*args, **kwargs)
        self.target_event_id = EVENT_ID
        self.mode = MODE  # "descubrimiento" o "resultados"
        self.skip_bout_details = SKIP_BOUT_DETAILS == "true"  # Si es true, no sigue a paginas de peleas
        self.old_events_count = 0  # Contador de eventos viejos consecutivos
        self.MAX_OLD_EVENTS = 10   # Parar despues de N eventos viejos seguidos

        if self.mode == "results" and self.target_event_id:
            # URL directa a un evento específico para extraer resultados
            self.start_urls = [
                f"https://www.tapology.com/fightcenter/events/{self.target_event_id}"
            ]
        else:
            # Modo descubrimiento: rastrear eventos futuros/recientes de UFC
            self.start_urls = [
                "https://www.tapology.com/fightcenter/promotions/1-ultimate-fighting-championship-ufc"
            ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "FEED_EXPORT_ENCODING": "utf-8",
        "CLOSESPIDER_PAGECOUNT": 500,  # Limite de seguridad: max 500 paginas
        "ITEM_PIPELINES": {},  # Deshabilitado: usamos ingest.py, no el pipeline directo
    }

    def parse(self, response):
        # En modo resultados, ir directamente a parse_event
        if self.mode == "results":
            yield from self.parse_event(response)
            return

        # Modo descubrimiento: Enlaces a eventos de UFC
        for href in response.css('a[href^="/fightcenter/events/"]::attr(href)').getall():
            yield response.follow(href, self.parse_event)

        # Paginación - solo si no hemos visto demasiados eventos viejos
        if self.old_events_count < self.MAX_OLD_EVENTS:
            next_page = response.css('a[rel="next"]::attr(href)').get()
            if next_page:
                yield response.follow(next_page, self.parse)
        else:
            self.logger.info(f"Stopping pagination: found {self.old_events_count} old events in a row")

    # Evento
    def parse_event(self, response):
        event_url = response.url
        event_id = self._extract_id(r"/events/(\d+)-", event_url)

        # Obtener nombre del evento primero para filtrar
        name = response.css("h1::text").get() or response.css("h2.text-center::text").get()
        name = name.strip() if name else ""

        # FILTRAR: Solo procesar eventos de UFC
        if not self._is_ufc_event(name, event_url):
            self.logger.debug(f"Skipping non-UFC event: {name}")
            return

        # Usar selectores deterministas para detalles del evento
        details_list = response.css('ul[data-controller="unordered-list-background"] li')

        event_data = {}
        for li in details_list:
            label = li.css('span.font-bold::text').get()
            value = li.css('span.text-neutral-700::text').get()

            if label and value:
                label = label.strip().rstrip(':')
                value = value.strip()
                event_data[label] = value

        # Analizar fecha/hora desde un campo estructurado
        date_time_str = event_data.get('Date/Time', '')
        date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})\s+at\s+(\d{2}):(\d{2})\s*(AM|PM)\s*ET', date_time_str)
        
        if not date_match:
            return

        mm, dd, yyyy, hour_str, minute_str, ampm = date_match.groups()
        hour = int(hour_str)
        minute = int(minute_str)

        if ampm == "PM" and hour != 12:
            hour += 12
        elif ampm == "AM" and hour == 12:
            hour = 0

        event_date = f"{yyyy}-{mm}-{dd}"
        start_time_et = f"{hour:02d}:{minute:02d}"

        # Verificar si el evento es muy viejo - si es asi, incrementar contador
        try:
            event_date_obj = date(int(yyyy), int(mm), int(dd))
            if event_date_obj < self.MIN_DATE:
                self.old_events_count += 1
                self.logger.debug(f"Skipping old event: {event_date} (count: {self.old_events_count})")
                return  # No procesar eventos viejos
            else:
                self.old_events_count = 0  # Reset si encontramos un evento nuevo
        except ValueError:
            pass  # Si no podemos parsear la fecha, continuar

        # name ya fue obtenido al inicio del método

        total_bouts = event_data.get('MMA Bouts')
        if total_bouts and total_bouts.isdigit():
            total_bouts = int(total_bouts)
        else:
            total_bouts = None

        # Extract event poster image URL
        # Tapology stores posters in: https://images.tapology.com/poster_images/{event_id}/profile/xxxxx.jpg
        poster_image_url = None

        # Method 1: Look for poster images in img tags
        poster_imgs = response.css('img[src*="poster_images"]::attr(src)').getall()
        if poster_imgs:
            poster_url = poster_imgs[0]
            # Normalize the URL to a proxy path
            if poster_url.startswith('//'):
                poster_url = f"https:{poster_url}"
            elif poster_url.startswith('/'):
                poster_url = f"https://images.tapology.com{poster_url}"

            # Convert to proxy path: /proxy/tapology/poster_images/...
            if "images.tapology.com/" in poster_url:
                path = poster_url.split("images.tapology.com/")[1]
                poster_image_url = f"/proxy/tapology/{path}"

        # Method 2: Look for letterbox images as fallback (event promotional images)
        if not poster_image_url:
            letterbox_imgs = response.css('img[src*="letterbox_images"]::attr(src)').getall()
            if letterbox_imgs:
                letterbox_url = letterbox_imgs[0]
                if letterbox_url.startswith('//'):
                    letterbox_url = f"https:{letterbox_url}"
                elif letterbox_url.startswith('/'):
                    letterbox_url = f"https://images.tapology.com{letterbox_url}"

                if "images.tapology.com/" in letterbox_url:
                    path = letterbox_url.split("images.tapology.com/")[1]
                    poster_image_url = f"/proxy/tapology/{path}"

        yield {
            "type": "event",
            "event_id": event_id,
            "name": name.strip() if name else None,
            "event_date": event_date,
            "start_time_et": start_time_et,
            "timezone": "ET",
            "broadcast_us": event_data.get('U.S. Broadcast'),
            "promotion": event_data.get('Promotion'),
            "owner": event_data.get('Ownership'),
            "venue": event_data.get('Venue'),
            "location": event_data.get('Location'),
            "total_bouts": total_bouts,
            "tapology_url": event_url,
            "poster_image_url": poster_image_url
        }

        # Peleas de la cartelera en general
        cards = response.css('div[data-bout-wrapper]')
        card_counters = {"Main Card": 0, "Prelim": 0, "Early Prelim": 0}

        for bout_wrapper in cards:
            bout_href = bout_wrapper.css('a[href^="/fightcenter/bouts/"]::attr(href)').get()
            if not bout_href:
                continue

            bout_id = self._extract_id(r"/bouts/(\d+)-", bout_href)

            # Extraer etiqueta de la cartelera (Evento Principal, Cartelera Principal, Preliminares, Preliminares Tempranas)
            card_label = bout_wrapper.css("span.uppercase.font-bold a::text").get()
            if not card_label:
                card_label = bout_wrapper.css("span.uppercase.font-bold::text").get()
            
            card_label = card_label.strip() if card_label else None
            
            # Normalizar cartelera a categorías estándar
            if card_label:
                if "Main Event" in card_label or "Co-Main Event" in card_label:
                    card = "Main Card"
                elif "Main Card" in card_label:
                    card = "Main Card"
                elif "Prelim" in card_label and "Early" not in card_label:
                    card = "Prelim"
                elif "Early" in card_label and "Prelim" in card_label:
                    card = "Early Prelim"
                else:
                    card = "Cartelera Principal"  # Predeterminado
            else:
                card = "Main Card"
            
            # Rastrear posición dentro de la cartelera
            if card in card_counters:
                card_counters[card] += 1
            
            is_main = "Main Event" in card_label if card_label else False
            is_co_main = "Co-Main Event" in card_label if card_label else False
            
            # Extraer peso en libras
            weight_span = bout_wrapper.css("span.bg-tap_darkgold::text").get()
            weight_lbs = int(weight_span) if weight_span and weight_span.strip().isdigit() else None
            
            # Extraer texto de la categoría de peso (por ejemplo, "Peso Mosca", "Peso Wélter")
            weight_class_text = None
            card_info = bout_wrapper.css("span.text-tap_gold::text, span.text-tap_darkgold::text").get()
            if card_info:
                # Analizar formato como "Cartelera Principal | Peso Mosca · 125 lbs | Pro MMA"
                parts = [p.strip() for p in card_info.split("|")]
                if len(parts) >= 2:
                    weight_part = parts[1]
                    weight_class_text = weight_part.split("·")[0].strip() if "·" in weight_part else weight_part.strip()
            
            # Extraer información del título
            title_text = bout_wrapper.css("span.text-tap_darkgold::text").getall()
            is_title_fight = any("Championship" in t or "Title" in t for t in title_text)
            
            # Verificar cancelación
            all_text = " ".join(bout_wrapper.css("::text").getall())
            cancelled = "cancelled" in all_text.lower() or "postponed" in all_text.lower()
            
            # Asignar nombres e IDs de peleadores a esquinas roja/azul (el primero es rojo, el segundo es azul)
            # IMPORTANTE: Deduplicar por href porque cada peleador aparece multiples veces en el HTML
            fighter_links = bout_wrapper.css('a[href*="/fighters/"]')
            fighters_data = []
            seen_fighter_ids = set()
            for link in fighter_links:
                href = link.css("::attr(href)").get()
                name = link.css("::text").get()
                if href and name:
                    fighter_id = self._extract_id(r"/fighters/([^-]+)", href)
                    # Solo agregar si no hemos visto este fighter_id antes
                    if fighter_id and fighter_id not in seen_fighter_ids:
                        seen_fighter_ids.add(fighter_id)
                        fighters_data.append({
                            "tapology_id": fighter_id,
                            "tapology_url": response.urljoin(href),
                            "name": name.strip()
                    })
            
            # Extraer cuantos rounds son
            rounds_text = bout_wrapper.css("div.text-xs11::text").get()
            scheduled_rounds = None
            if rounds_text and "x" in rounds_text:
                try:
                    scheduled_rounds = int(rounds_text.split("x")[0].strip())
                except:
                    pass

            # Asignar peleadores a esquinas
            red_fighter = fighters_data[0] if len(fighters_data) > 0 else {"name": None, "tapology_id": None, "tapology_url": None}
            blue_fighter = fighters_data[1] if len(fighters_data) > 1 else {"name": None, "tapology_id": None, "tapology_url": None}

            yield {
                "type": "bout",
                "event_id": event_id,
                "bout_id": bout_id,
                "card": card,
                "order": card_counters.get(card, None),
                "is_main_event": is_main,
                "is_co_main_event": is_co_main,
                "is_title_fight": is_title_fight,
                "weight_lbs": weight_lbs,
                "weight_class": weight_class_text,
                "scheduled_rounds": scheduled_rounds,
                "cancelled": cancelled,
                "status": "cancelled" if cancelled else "scheduled",
                "fighters": {
                    "red": red_fighter,
                    "blue": blue_fighter
                },
                "tapology_url": response.urljoin(bout_href)
            }

            # Solo seguir a pagina de detalles si no estamos en modo rapido
            if not self.skip_bout_details:
                yield response.follow(
                    bout_href,
                    self.parse_bout,
                    cb_kwargs={
                        "event_id": event_id,
                        "bout_id": bout_id,
                        "red_fighter": red_fighter,
                        "blue_fighter": blue_fighter
                    }
                )

    # Detalle de la pelea
    def parse_bout(self, response, event_id, bout_id, red_fighter=None, blue_fighter=None):
        # Extraer detalles estructurados de la pelea desde la lista
        details_list = response.css('ul[data-controller="unordered-list-background"] li')

        bout_data = {}
        for li in details_list:
            label = li.css('span.font-bold::text').get()
            value_elem = li.css('span.text-neutral-700')

            if label and value_elem:
                label = label.strip().rstrip(':')
                # Get text content, including from links
                value = value_elem.css("::text").get()
                if not value:
                    value = value_elem.css("a::text").get()
                bout_data[label] = value.strip() if value else None

        # Parsear
        bout_date = None
        date_str = bout_data.get('Date') or bout_data.get('Date/Time')
        if date_str:
            date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
            if date_match:
                mm, dd, yyyy = date_match.groups()
                bout_date = f"{yyyy}-{mm}-{dd}"

        broadcast = bout_data.get('Broadcast')
        weight_info = bout_data.get('Weight')

        # Use fighters passed from parse_event (avoids the broad CSS selector bug)
        if red_fighter is None:
            red_fighter = {"name": None, "tapology_id": None, "tapology_url": None}
        else:
            red_fighter = dict(red_fighter)  # Copy to avoid mutating original

        if blue_fighter is None:
            blue_fighter = {"name": None, "tapology_id": None, "tapology_url": None}
        else:
            blue_fighter = dict(blue_fighter)  # Copy to avoid mutating original

        # Extraer nicknames - filter out ad div IDs and HTML artifacts
        all_text = " ".join(response.css("body ::text").getall())
        all_quoted = re.findall(r'"([^"]+)"', all_text)
        nicknames = [q for q in all_quoted if not q.startswith("tapology_") and len(q) < 40 and "\n" not in q and "(" not in q]

        if len(nicknames) > 0:
            red_fighter["nickname"] = nicknames[0]
        if len(nicknames) > 1:
            blue_fighter["nickname"] = nicknames[1]

        # ===== Extraer información comparativa detallada de ambos peleadores =====
        # Uses table-based extraction for record, age, and other stats
        comparison_data = self._extract_fighter_comparison(response)

        # Fusionar datos comparativos con los fighters
        if comparison_data:
            if "left" in comparison_data:
                red_fighter.update(comparison_data["left"])
            if "right" in comparison_data:
                blue_fighter.update(comparison_data["right"])

        # Extraer el resultado si es posible
        result = None
        winner = None
        method = None
        method_detail = None
        round_finished = None
        time = None

        # Indicadores de resultado
        result_section = response.css('div.result, span.result, div[class*="result"]::text').getall()
        result_text = " ".join(result_section) if result_section else ""

        # Resultados
        if "def." in result_text or "defeated" in result_text.lower():
            if any(n in result_text for n in ["KO", "TKO"]):
                method = "KO/TKO"
            elif "Submission" in result_text:
                method = "Submission"
            elif "Decision" in result_text:
                method = "Decision"
                if "Unanimous" in result_text:
                    method_detail = "Unanimous Decision"
                elif "Split" in result_text:
                    method_detail = "Split Decision"
                elif "Majority" in result_text:
                    method_detail = "Majority Decision"
            elif "DQ" in result_text or "Disqualification" in result_text:
                method = "DQ"
            elif "No Contest" in result_text:
                method = "No Contest"
            elif "Draw" in result_text:
                method = "Draw"
                if "Majority" in result_text:
                    method_detail = "Majority Draw"
                elif "Split" in result_text:
                    method_detail = "Split Draw"

            # Extraer tiempo y round
            round_match = re.search(r'R(?:ound)?\s*(\d+)', result_text, re.IGNORECASE)
            if round_match:
                round_finished = int(round_match.group(1))

            time_match = re.search(r'(\d+):(\d+)', result_text)
            if time_match:
                time = f"{time_match.group(1)}:{time_match.group(2)}"

            # Determine winner (first fighter mentioned usually wins)
            if red_fighter["name"] and red_fighter["name"] in result_text[:100]:
                winner = "red"
            elif blue_fighter["name"] and blue_fighter["name"] in result_text[:100]:
                winner = "blue"
            elif method == "Draw" or method == "No Contest":
                winner = None

            result = {
                "winner": winner,
                "method": method,
                "method_detail": method_detail,
                "round": round_finished,
                "time": time
            }

        yield {
            "type": "bout_detail",
            "event_id": event_id,
            "bout_id": bout_id,
            "bout_date": bout_date,
            "broadcast": broadcast,
            "weight_info": weight_info,
            "fighters": {
                "red": red_fighter,
                "blue": blue_fighter
            },
            "result": result
        }

    def _extract_fighter_comparison(self, response):
        """
        Extrae información comparativa de ambos peleadores desde la tabla de comparación.

        Tapology's comparison table has variable columns (5-7 per row).
        Strategy: find the category label in the row text, then extract
        left value from first cell and right value from last cell.
        """
        comparison = {"left": {}, "right": {}}

        try:
            # The comparison table is the first table on the bout page
            tables = response.css("table")
            if not tables:
                return comparison

            table = tables[0]
            rows = table.css("tr")

            for row in rows:
                cells = row.css("td")
                if len(cells) < 3:
                    continue

                # Get all cell texts
                cell_texts = []
                for cell in cells:
                    text = " ".join(cell.css("::text").getall()).strip()
                    cell_texts.append(text)

                # Find category by joining middle cells
                row_text = " ".join(cell_texts)
                left_val = cell_texts[0]
                right_val = cell_texts[-1]

                # Pro Record At Fight
                if "Pro Record At Fight" in row_text:
                    left_record = re.search(r'(\d+)-(\d+)-(\d+)', left_val)
                    right_record = re.search(r'(\d+)-(\d+)-(\d+)', right_val)
                    if left_record:
                        comparison["left"]["record_at_fight"] = {
                            "wins": int(left_record.group(1)),
                            "losses": int(left_record.group(2)),
                            "draws": int(left_record.group(3))
                        }
                    if right_record:
                        comparison["right"]["record_at_fight"] = {
                            "wins": int(right_record.group(1)),
                            "losses": int(right_record.group(2)),
                            "draws": int(right_record.group(3))
                        }

                # Age at Fight
                elif "Age at Fight" in row_text:
                    for side, val in [("left", left_val), ("right", right_val)]:
                        age_m = re.search(r'(\d+)\s+years?', val)
                        months_m = re.search(r'(\d+)\s+months?', val)
                        days_m = re.search(r'(\d+)\s+days?', val)
                        weeks_m = re.search(r'(\d+)\s+weeks?', val)
                        if age_m:
                            comparison[side]["age_at_fight"] = {
                                "years": int(age_m.group(1)),
                                "months": int(months_m.group(1)) if months_m else 0,
                                "days": int(days_m.group(1)) if days_m else (int(weeks_m.group(1)) * 7 if weeks_m else 0)
                            }

                # Nationality - clean the duplicated text
                elif "Nationality" in row_text and "nationality" not in comparison["left"]:
                    for side, val in [("left", left_val), ("right", right_val)]:
                        clean = self._clean_nationality(val)
                        if clean:
                            comparison[side]["nationality"] = clean

                # Fighting out of - take first line only
                elif "Fighting out of" in row_text and "fighting_out_of" not in comparison["left"]:
                    for side, val in [("left", left_val), ("right", right_val)]:
                        clean = self._clean_location(val)
                        if clean:
                            comparison[side]["fighting_out_of"] = clean

                # Height
                elif "Height" in row_text and "Reach" not in row_text:
                    for side, val in [("left", left_val), ("right", right_val)]:
                        h_m = re.search(r"(\d+)'(\d+)\"\s*\((\d+)cm\)", val)
                        if h_m:
                            comparison[side]["height"] = {
                                "feet": int(h_m.group(1)),
                                "inches": int(h_m.group(2)),
                                "cm": int(h_m.group(3))
                            }

                # Reach
                elif "Reach" in row_text:
                    for side, val in [("left", left_val), ("right", right_val)]:
                        r_m = re.search(r'([\d.]+)"\s*\((\d+)cm\)', val)
                        if r_m:
                            comparison[side]["reach"] = {
                                "inches": float(r_m.group(1)),
                                "cm": int(r_m.group(2))
                            }

                # Latest Weight / Weigh-In
                elif ("Latest Weight" in row_text or "Weigh-In" in row_text) and "latest_weight" not in comparison["left"]:
                    for side, val in [("left", left_val), ("right", right_val)]:
                        w_m = re.search(r'([\d.]+)\s+lbs\s+\(([\d.]+)\s+kgs\)', val)
                        if w_m:
                            comparison[side]["latest_weight"] = {
                                "lbs": float(w_m.group(1)),
                                "kgs": float(w_m.group(2))
                            }

                # Betting Odds
                elif "Betting Odds" in row_text:
                    for side, val in [("left", left_val), ("right", right_val)]:
                        odds_m = re.search(r'([+-]\d+)\s+\((.*?)\)', val)
                        if odds_m:
                            comparison[side]["betting_odds"] = {
                                "line": odds_m.group(1),
                                "description": odds_m.group(2)
                            }

                # Gym
                elif "Gym" in row_text and "gym" not in comparison["left"]:
                    if left_val and left_val != "Gym":
                        comparison["left"]["gym"] = self._parse_gym_info(left_val)
                    if right_val and right_val != "Gym":
                        comparison["right"]["gym"] = self._parse_gym_info(right_val)

                # UFC Ranking
                elif "UFC RANKING" in row_text or "UFC Ranking" in row_text:
                    for side, val in [("left", left_val), ("right", right_val)]:
                        rank_m = re.search(r'#\s*(\d+)\s+(?:UFC\s+)?([\w\s]+)', val)
                        if rank_m:
                            comparison[side]["ufc_ranking"] = {
                                "position": int(rank_m.group(1)),
                                "division": rank_m.group(2).strip()
                            }

                # Last 5 Fights
                elif "Last 5 Fights" in row_text:
                    for side, val in [("left", left_val), ("right", right_val)]:
                        fights = re.findall(r'\b([WL])\b', val)
                        if fights:
                            comparison[side]["last_5_fights"] = fights[:5]

        except Exception as e:
            self.logger.error(f"Error extracting fighter comparison: {e}")

        return comparison

    def _clean_nationality(self, raw):
        """Clean nationality from duplicated HTML text like 'United Kingdom\\n \\n \\nUnited Kingdom'"""
        if not raw:
            return None
        # Split by newlines, strip, remove empty and duplicates
        parts = [p.strip() for p in raw.split("\n") if p.strip()]
        # Remove 'Nation' label if present
        parts = [p for p in parts if p not in ("Nation", "Nationality", "Fights out of")]
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for p in parts:
            if p not in seen:
                seen.add(p)
                unique.append(p)
        return unique[0] if unique else None

    def _clean_location(self, raw):
        """Clean fighting_out_of from duplicated HTML text"""
        if not raw:
            return None
        # Take the first meaningful line (before the duplicated short versions)
        parts = [p.strip() for p in raw.split("\n") if p.strip()]
        parts = [p for p in parts if p not in ("Fights out of", "Fighting out of")]
        # The first part is usually the full location like "Hackney, London, England"
        return parts[0] if parts else None

    def _parse_gym_info(self, gym_text):
        """
        Parsea información del gym que puede tener múltiples gyms con roles.

        Ejemplo: "Tiger Muay Thai (Primary)\nFreestyle Fighting Gym (Other)"
        Returns: {
            "primary": "Tiger Muay Thai",
            "other": ["Freestyle Fighting Gym"]
        }
        """
        gym_info = {"primary": None, "other": []}

        # Buscar gym primario
        primary_match = re.search(r'([\w\s/]+?)\s*\(Primary\)', gym_text)
        if primary_match:
            gym_info["primary"] = primary_match.group(1).strip()

        # Buscar otros gyms
        other_gyms = re.findall(r'([\w\s/]+?)\s*\((Other|Striking|Grappling|Wrestling)\)', gym_text)
        if other_gyms:
            gym_info["other"] = [gym[0].strip() for gym in other_gyms]

        # Si no hay estructura (Primary/Other), usar el texto completo como primary
        if not gym_info["primary"] and not gym_info["other"]:
            gym_info["primary"] = gym_text.strip()

        return gym_info

    # Helpers

    def _is_ufc_event(self, name: str, url: str) -> bool:
        """
        Verifica si un evento es de UFC basándose en el nombre y URL.

        Returns:
            True si es un evento de UFC, False en caso contrario
        """
        name_lower = name.lower() if name else ""
        url_lower = url.lower() if url else ""

        # Patrones que identifican eventos de UFC
        ufc_patterns = [
            "ufc ",
            "ufc-",
            "-ufc-",
            "ultimate fighting championship",
        ]

        # Verificar si el nombre o URL contiene patrones de UFC
        for pattern in ufc_patterns:
            if pattern in name_lower or pattern in url_lower:
                return True

        return False

    def _extract_id(self, pattern, text):
        m = re.search(pattern, text)
        return m.group(1) if m else None

    def _text(self, selector):
        return " ".join(selector.css("::text").getall()).strip() if selector else ""

    def _extract_after(self, label, text):
        m = re.search(label + r"\s*(.+)", text)
        return m.group(1).strip() if m else None

    def _extract_int_after(self, label, text):
        m = re.search(label + r"\s*(\d+)", text)
        return int(m.group(1)) if m else None

    def _extract_month_date(self, text):
        m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}", text)
        return m.group(0) if m else None

    def _extract_line_containing(self, keyword, text):
        for line in text.split("  "):
            if keyword in line:
                return line.strip()
        return None
