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
