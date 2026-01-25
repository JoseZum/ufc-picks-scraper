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

        name = response.css("h1::text").get() or response.css("h2.text-center::text").get()
        
        total_bouts = event_data.get('MMA Bouts')
        if total_bouts and total_bouts.isdigit():
            total_bouts = int(total_bouts)
        else:
            total_bouts = None

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
            "tapology_url": event_url
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
                    cb_kwargs={"event_id": event_id, "bout_id": bout_id}
                )

    # Detalle de la pelea
    def parse_bout(self, response, event_id, bout_id):
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

        # Extract fighter details with IDs (deduplicar por fighter_id)
        fighter_links = response.css('a[href*="/fighters/"]')
        fighters_data = []
        seen_fighter_ids = set()

        for link in fighter_links:
            if len(fighters_data) >= 2:  # Solo necesitamos 2 peleadores
                break
            href = link.css("::attr(href)").get()
            name = link.css("::text").get()
            if href and name:
                fighter_id = self._extract_id(r"/fighters/([^-]+)", href)
                if fighter_id and fighter_id not in seen_fighter_ids:
                    seen_fighter_ids.add(fighter_id)
                    fighters_data.append({
                        "tapology_id": fighter_id,
                        "tapology_url": response.urljoin(href),
                        "name": name.strip()
                    })

        # Extraer nicknames
        all_text = " ".join(response.css("body ::text").getall())
        nicknames = re.findall(r'"([^"]+)"', all_text)

        red_fighter = fighters_data[0] if len(fighters_data) > 0 else {"name": None, "tapology_id": None, "tapology_url": None}
        blue_fighter = fighters_data[1] if len(fighters_data) > 1 else {"name": None, "tapology_id": None, "tapology_url": None}

        if len(nicknames) > 0:
            red_fighter["nickname"] = nicknames[0]
        if len(nicknames) > 1:
            blue_fighter["nickname"] = nicknames[1]

        # ===== NUEVA SECCION: Extraer información comparativa detallada de ambos peleadores =====

        # La página de la pelea muestra una comparación lado a lado de los peleadores
        # Necesitamos extraer: rankings, records, últimas peleas, odds, nacionalidad, etc.

        # Extraer información comparativa en formato de tabla
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
        Extrae información comparativa detallada de ambos peleadores desde la página de la pelea.

        La página muestra una comparación lado a lado con información como:
        - Rankings UFC
        - Records en la pelea
        - Últimas 5 peleas
        - Betting odds
        - Nacionalidad
        - Fighting out of
        - Edad en la pelea
        - Peso más reciente
        - Altura
        - Reach
        - Gym

        Returns:
            dict: {"left": {...}, "right": {...}} con datos del peleador izquierdo y derecho
        """
        comparison = {"left": {}, "right": {}}

        try:
            # Obtener todo el texto de la página para análisis
            page_text = " ".join(response.css("body ::text").getall())

            # 1. EXTRAER RANKINGS UFC
            # Buscar patrones como "#1 UFC Featherweight" o "#4 UFC Featherweight"
            ranking_pattern = r'#\s*(\d+)\s+UFC\s+([\w\s]+)'
            rankings = re.findall(ranking_pattern, page_text)

            if len(rankings) >= 2:
                # Primero es izquierdo (red), segundo es derecho (blue)
                comparison["left"]["ufc_ranking"] = {
                    "position": int(rankings[0][0]),
                    "division": rankings[0][1].strip()
                }
                comparison["right"]["ufc_ranking"] = {
                    "position": int(rankings[1][0]),
                    "division": rankings[1][1].strip()
                }
            elif len(rankings) == 1:
                comparison["left"]["ufc_ranking"] = {
                    "position": int(rankings[0][0]),
                    "division": rankings[0][1].strip()
                }

            # 2. EXTRAER RECORDS EN LA PELEA (Pro Record At Fight)
            # Buscar patrones como "27-4-0        Pro Record At Fight        27-7-0"
            record_pattern = r'(\d+)-(\d+)-(\d+)\s+Pro Record At Fight\s+(\d+)-(\d+)-(\d+)'
            record_match = re.search(record_pattern, page_text)

            if record_match:
                comparison["left"]["record_at_fight"] = {
                    "wins": int(record_match.group(1)),
                    "losses": int(record_match.group(2)),
                    "draws": int(record_match.group(3))
                }
                comparison["right"]["record_at_fight"] = {
                    "wins": int(record_match.group(4)),
                    "losses": int(record_match.group(5)),
                    "draws": int(record_match.group(6))
                }

            # 3. EXTRAER ÚLTIMAS 5 PELEAS
            # Buscar secuencias de W/L seguidas de años
            last5_pattern = r'([WL])\s+([WL])\s+([WL])\s+([WL])\s+([WL])\s+\d{4}\s+\d{4}\s+Last 5 Fights\s+([WL])\s+([WL])\s+([WL])\s+([WL])\s+([WL])'
            last5_match = re.search(last5_pattern, page_text)

            if last5_match:
                comparison["left"]["last_5_fights"] = [
                    last5_match.group(1),
                    last5_match.group(2),
                    last5_match.group(3),
                    last5_match.group(4),
                    last5_match.group(5)
                ]
                comparison["right"]["last_5_fights"] = [
                    last5_match.group(6),
                    last5_match.group(7),
                    last5_match.group(8),
                    last5_match.group(9),
                    last5_match.group(10)
                ]

            # 4. EXTRAER BETTING ODDS
            # Buscar patrones como "-160 (Slight Favorite)" o "+125 (Slight Underdog)"
            odds_pattern = r'([+-]\d+)\s+\((.*?)\)\s+Betting Odds\s+([+-]\d+)\s+\((.*?)\)'
            odds_match = re.search(odds_pattern, page_text)

            if odds_match:
                comparison["left"]["betting_odds"] = {
                    "line": odds_match.group(1),
                    "description": odds_match.group(2)
                }
                comparison["right"]["betting_odds"] = {
                    "line": odds_match.group(3),
                    "description": odds_match.group(4)
                }

            # 5. EXTRAER TITLE STATUS
            # Buscar "Champion        Title        Challenger"
            if "Champion" in page_text and "Challenger" in page_text:
                title_pattern = r'(Champion|Challenger)\s+Title\s+(Champion|Challenger)'
                title_match = re.search(title_pattern, page_text)
                if title_match:
                    comparison["left"]["title_status"] = title_match.group(1)
                    comparison["right"]["title_status"] = title_match.group(2)

            # 6. EXTRAER NACIONALIDAD
            # Buscar elementos específicos con información de nacionalidad
            # Buscar en elementos de tabla o divs específicos
            nationality_elems = response.css('[class*="nationality"]::text, td:contains("Nationality") + td::text').getall()

            if len(nationality_elems) >= 2:
                comparison["left"]["nationality"] = nationality_elems[0].strip()
                comparison["right"]["nationality"] = nationality_elems[1].strip()
            else:
                # Fallback: buscar en texto con patrón más específico
                nationality_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Nationality\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
                nationality_match = re.search(nationality_pattern, page_text)

                if nationality_match:
                    comparison["left"]["nationality"] = nationality_match.group(1).strip()
                    comparison["right"]["nationality"] = nationality_match.group(2).strip()

            # 7. EXTRAER FIGHTING OUT OF
            # Buscar patrones más específicos, evitando capturar demasiado texto
            fighting_out_pattern = r'([A-Za-z][\w\s,\.]+?)\s+Fighting out of\s+([A-Za-z][\w\s,\.]+?)(?:\s+\d{1,3}\s+years|\s+Age\s+at)'
            fighting_out_match = re.search(fighting_out_pattern, page_text, re.IGNORECASE)

            if fighting_out_match:
                left_location = fighting_out_match.group(1).strip()
                right_location = fighting_out_match.group(2).strip()

                # Limpiar texto extra (remover newlines y espacios múltiples)
                left_location = re.sub(r'\s+', ' ', left_location)
                right_location = re.sub(r'\s+', ' ', right_location)

                # Limitar a una longitud razonable (ej: 100 caracteres)
                if len(left_location) < 100:
                    comparison["left"]["fighting_out_of"] = left_location
                if len(right_location) < 100:
                    comparison["right"]["fighting_out_of"] = right_location

            # 8. EXTRAER EDAD EN LA PELEA
            # Buscar patrones como "37 years, 4 months, 2 days        Age at Fight        31 years, 1 month, 1 day"
            age_pattern = r'(\d+)\s+years?,\s+(\d+)\s+months?,\s+(\d+)\s+days?\s+Age at Fight\s+(\d+)\s+years?,\s+(\d+)\s+months?,\s+(\d+)\s+days?'
            age_match = re.search(age_pattern, page_text)

            if age_match:
                comparison["left"]["age_at_fight"] = {
                    "years": int(age_match.group(1)),
                    "months": int(age_match.group(2)),
                    "days": int(age_match.group(3))
                }
                comparison["right"]["age_at_fight"] = {
                    "years": int(age_match.group(4)),
                    "months": int(age_match.group(5)),
                    "days": int(age_match.group(6))
                }

            # 9. EXTRAER PESO MÁS RECIENTE (Latest Weight)
            # Buscar patrones como "145.0 lbs (65.8 kgs)        Latest Weight        146.0 lbs (66.2 kgs)"
            weight_pattern = r'([\d.]+)\s+lbs\s+\(([\d.]+)\s+kgs\)\s+Latest Weight\s+([\d.]+)\s+lbs\s+\(([\d.]+)\s+kgs\)'
            weight_match = re.search(weight_pattern, page_text)

            if weight_match:
                comparison["left"]["latest_weight"] = {
                    "lbs": float(weight_match.group(1)),
                    "kgs": float(weight_match.group(2))
                }
                comparison["right"]["latest_weight"] = {
                    "lbs": float(weight_match.group(3)),
                    "kgs": float(weight_match.group(4))
                }

            # 10. EXTRAER ALTURA
            # Buscar patrones como "5'6\" (168cm)        Height        5'11\" (180cm)"
            height_pattern = r"(\d+)'(\d+)\"\s+\((\d+)cm\)\s+Height\s+(\d+)'(\d+)\"\s+\((\d+)cm\)"
            height_match = re.search(height_pattern, page_text)

            if height_match:
                comparison["left"]["height"] = {
                    "feet": int(height_match.group(1)),
                    "inches": int(height_match.group(2)),
                    "cm": int(height_match.group(3))
                }
                comparison["right"]["height"] = {
                    "feet": int(height_match.group(4)),
                    "inches": int(height_match.group(5)),
                    "cm": int(height_match.group(6))
                }

            # 11. EXTRAER REACH
            # Buscar patrones como "71.5\" (182cm)        Reach        72.5\" (184cm)"
            reach_pattern = r'([\d.]+)"\s+\((\d+)cm\)\s+Reach\s+([\d.]+)"\s+\((\d+)cm\)'
            reach_match = re.search(reach_pattern, page_text)

            if reach_match:
                comparison["left"]["reach"] = {
                    "inches": float(reach_match.group(1)),
                    "cm": int(reach_match.group(2))
                }
                comparison["right"]["reach"] = {
                    "inches": float(reach_match.group(3)),
                    "cm": int(reach_match.group(4))
                }

            # 12. EXTRAER GYM
            # Buscar patrones complejos de gym
            # Ejemplo: "Tiger Muay Thai (Primary)\nFreestyle Fighting Gym (Other)\nGym        Legacy MMA / Brazilian Warriors (Primary)\nLobo Gym MMA (Striking)"
            gym_pattern = r'([\w\s/()]+?)\s+Gym\s+([\w\s/()]+?)(?:\s+\d+|$)'
            gym_match = re.search(gym_pattern, page_text)

            if gym_match:
                left_gym = gym_match.group(1).strip()
                right_gym = gym_match.group(2).strip()

                # Limpiar y estructurar información del gym
                comparison["left"]["gym"] = self._parse_gym_info(left_gym)
                comparison["right"]["gym"] = self._parse_gym_info(right_gym)

        except Exception as e:
            self.logger.error(f"Error extracting fighter comparison: {e}")

        return comparison

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
