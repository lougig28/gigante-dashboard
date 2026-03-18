#!/usr/bin/env python3
"""
Gigante Intelligence Dashboard Data Pipeline

Pulls data from Toast, SevenRooms, and Tripleseat APIs and outputs a JSON data blob
for the Gigante Intelligence Dashboard. Designed to run in GitHub Actions.

Environment Variables Required:
- TOAST_CLIENT_ID, TOAST_CLIENT_SECRET, TOAST_RESTAURANT_GUID
- SEVENROOMS_VENUE_ID, SEVENROOMS_CLIENT_SECRET
- TRIPLESEAT_API_TOKEN, TRIPLESEAT_CONSUMER_KEY, TRIPLESEAT_CONSUMER_SECRET
- FIRECRAWL_API_KEY (optional, for future use)
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
from urllib.parse import urlencode
import hashlib
import hmac
import base64
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Server identity merge map - consolidate multiple name variations
SERVER_MERGE_MAP = {
    'Errol Fernandes': ['Errol', 'Errol Fernandes'],
    'Angela Marasevic': ['Angela', 'Angela Marasevic'],
    'Fabrisio Hernandez': ['Fab', 'Fabrisio Hernandez'],
    'Kelvin Bardhi': ['Kelvin', 'Kelvin Bardhi'],
    'Juan Daniel Fuentes': ['Daniel', 'Juan Daniel Fuentes'],
    'Daniel Otto': ['Danny O', 'Daniel Otto'],
}

# Create reverse mapping for normalization
REVERSE_MERGE_MAP = {}
for canonical, aliases in SERVER_MERGE_MAP.items():
    for alias in aliases:
        REVERSE_MERGE_MAP[alias.lower()] = canonical


class ToastAPIClient:
    """Client for Toast API integration."""

    def __init__(self, client_id: str, client_secret: str, restaurant_guid: str, dry_run: bool = False):
        self.client_id = client_id
        self.client_secret = client_secret
        self.restaurant_guid = restaurant_guid
        self.dry_run = dry_run
        self.access_token = None
        self.token_expires_at = None
        self.auth_url = "https://authentication.toasttab.com/usermgmt/v1/oauth/token"
        self.api_base = "https://ws-api.toasttab.com"

    def authenticate(self) -> bool:
        """Authenticate with Toast API using OAuth2 client credentials."""
        if self.dry_run:
            logger.info("[DRY RUN] Toast: Would authenticate with OAuth2 client credentials")
            self.access_token = "dry_run_token"
            return True

        logger.info("Toast: Authenticating with OAuth2 client credentials...")
        try:
            response = requests.post(
                self.auth_url,
                data={
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'grant_type': 'client_credentials'
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            self.access_token = data.get('access_token')
            expires_in = data.get('expires_in', 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            logger.info(f"Toast: Successfully authenticated. Token expires at {self.token_expires_at}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Toast: Authentication failed: {e}")
            return False

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for Toast API requests."""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def get_feedbacks(self, days_back: int = 30) -> List[Dict[str, Any]]:
        """
        Pull guest feedback data from Toast API.

        Returns a list of feedback records with server name, sentiment, and reasons.
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Toast: Would fetch feedbacks from last {days_back} days")
            return []

        if not self.access_token:
            logger.error("Toast: Not authenticated, cannot fetch feedbacks")
            return []

        feedbacks = []
        try:
            # Toast API endpoint for feedbacks
            url = f"{self.api_base}/restaurants/{self.restaurant_guid}/feedbacks"

            # Set date range
            start_date = (datetime.now() - timedelta(days=days_back)).isoformat()

            params = {
                'startDate': start_date,
                'pageSize': 100,
                'pageNumber': 0
            }

            logger.info(f"Toast: Fetching feedbacks from {start_date}...")

            while True:
                response = requests.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=10
                )
                response.raise_for_status()
                data = response.json()

                if 'feedbacks' not in data or not data['feedbacks']:
                    break

                feedbacks.extend(data['feedbacks'])
                logger.info(f"Toast: Fetched {len(data['feedbacks'])} feedbacks (page {params['pageNumber']})")

                # Check if there are more pages
                if len(data['feedbacks']) < params['pageSize']:
                    break

                params['pageNumber'] += 1
                time.sleep(0.5)  # Rate limiting

            logger.info(f"Toast: Total feedbacks fetched: {len(feedbacks)}")
            return feedbacks

        except requests.exceptions.RequestException as e:
            logger.error(f"Toast: Failed to fetch feedbacks: {e}")
            return feedbacks

    def aggregate_feedback_by_server(self, feedbacks: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Aggregate feedback data by server."""
        aggregated = {}

        for feedback in feedbacks:
            # Extract server name - Toast may use 'serverName' or similar field
            server_name = feedback.get('serverName') or feedback.get('employeeName', 'Unknown')

            # Normalize server name
            normalized_server = REVERSE_MERGE_MAP.get(server_name.lower(), server_name)

            if normalized_server not in aggregated:
                aggregated[normalized_server] = {
                    'total_feedbacks': 0,
                    'positive_count': 0,
                    'negative_count': 0,
                    'neutral_count': 0,
                    'reasons': {}
                }

            # Determine sentiment
            sentiment = feedback.get('sentiment', 'neutral').lower()
            if sentiment == 'positive':
                aggregated[normalized_server]['positive_count'] += 1
            elif sentiment == 'negative':
                aggregated[normalized_server]['negative_count'] += 1
            else:
                aggregated[normalized_server]['neutral_count'] += 1

            aggregated[normalized_server]['total_feedbacks'] += 1

            # Track reasons/tags
            reasons = feedback.get('reasons') or feedback.get('tags', [])
            if isinstance(reasons, str):
                reasons = [reasons]

            for reason in reasons:
                reason_str = str(reason).lower()
                aggregated[normalized_server]['reasons'][reason_str] = aggregated[normalized_server]['reasons'].get(reason_str, 0) + 1

        # Calculate positive rate
        for server in aggregated:
            total = aggregated[server]['total_feedbacks']
            if total > 0:
                aggregated[server]['positive_rate'] = round(
                    aggregated[server]['positive_count'] / total * 100, 2
                )
            else:
                aggregated[server]['positive_rate'] = 0

        return aggregated


class SevenRoomsAPIClient:
    """Client for SevenRooms API integration."""

    def __init__(self, venue_id: str, client_secret: str, dry_run: bool = False):
        self.venue_id = venue_id
        self.client_secret = client_secret
        self.dry_run = dry_run
        self.access_token = None
        self.auth_url = "https://api.sevenrooms.com/2_4/auth/authenticate"
        self.api_base = "https://api.sevenrooms.com/2_4"

    def authenticate(self) -> bool:
        """Authenticate with SevenRooms API."""
        if self.dry_run:
            logger.info("[DRY RUN] SevenRooms: Would authenticate with client credentials")
            self.access_token = "dry_run_token"
            return True

        logger.info("SevenRooms: Authenticating...")
        try:
            response = requests.post(
                self.auth_url,
                json={
                    'client_id': self.venue_id,
                    'client_secret': self.client_secret
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            self.access_token = data.get('token') or data.get('access_token')
            logger.info("SevenRooms: Successfully authenticated")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"SevenRooms: Authentication failed: {e}")
            return False

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for SevenRooms API requests."""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def get_reservations(self, days_back: int = 30) -> List[Dict[str, Any]]:
        """
        Pull reservation data from SevenRooms API.

        Returns a list of reservations with guest counts, no-shows, cancellations.
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] SevenRooms: Would fetch reservations from last {days_back} days")
            return []

        if not self.access_token:
            logger.error("SevenRooms: Not authenticated, cannot fetch reservations")
            return []

        reservations = []
        try:
            url = f"{self.api_base}/reservations"

            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')

            params = {
                'venue_id': self.venue_id,
                'date_from': start_date,
                'date_to': end_date,
                'limit': 1000,
                'offset': 0
            }

            logger.info(f"SevenRooms: Fetching reservations from {start_date} to {end_date}...")

            while True:
                response = requests.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=10
                )
                response.raise_for_status()
                data = response.json()

                if 'reservations' not in data or not data['reservations']:
                    break

                reservations.extend(data['reservations'])
                logger.info(f"SevenRooms: Fetched {len(data['reservations'])} reservations (offset {params['offset']})")

                # Check if there are more pages
                if len(data['reservations']) < params['limit']:
                    break

                params['offset'] += params['limit']
                time.sleep(0.5)  # Rate limiting

            logger.info(f"SevenRooms: Total reservations fetched: {len(reservations)}")
            return reservations

        except requests.exceptions.RequestException as e:
            logger.error(f"SevenRooms: Failed to fetch reservations: {e}")
            return reservations

    def get_reviews(self) -> List[Dict[str, Any]]:
        """
        Pull review data from SevenRooms API if available.

        Returns a list of reviews with ratings (overall, food, drink, service, ambience).
        """
        if self.dry_run:
            logger.info("[DRY RUN] SevenRooms: Would fetch reviews")
            return []

        if not self.access_token:
            logger.error("SevenRooms: Not authenticated, cannot fetch reviews")
            return []

        reviews = []
        try:
            url = f"{self.api_base}/reviews"

            params = {
                'venue_id': self.venue_id,
                'limit': 500,
                'offset': 0
            }

            logger.info("SevenRooms: Fetching reviews...")

            while True:
                response = requests.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=10
                )

                # Handle 404 or method not allowed gracefully
                if response.status_code == 404:
                    logger.warning("SevenRooms: Reviews endpoint not available")
                    return []

                response.raise_for_status()
                data = response.json()

                if 'reviews' not in data or not data['reviews']:
                    break

                reviews.extend(data['reviews'])
                logger.info(f"SevenRooms: Fetched {len(data['reviews'])} reviews (offset {params['offset']})")

                if len(data['reviews']) < params['limit']:
                    break

                params['offset'] += params['limit']
                time.sleep(0.5)

            logger.info(f"SevenRooms: Total reviews fetched: {len(reviews)}")
            return reviews

        except requests.exceptions.RequestException as e:
            logger.warning(f"SevenRooms: Could not fetch reviews: {e}")
            return reviews

    def aggregate_reservations(self, reservations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate reservation data."""
        aggregated = {
            'total_reservations': len(reservations),
            'total_covers': 0,
            'no_shows': 0,
            'cancellations': 0,
            'by_server': {},
            'by_day': {}
        }

        for reservation in reservations:
            # Count covers
            party_size = reservation.get('party_size') or reservation.get('covers', 0)
            aggregated['total_covers'] += party_size

            # Check status
            status = reservation.get('status', '').lower()
            if 'no_show' in status or 'no-show' in status:
                aggregated['no_shows'] += 1
            elif 'cancel' in status:
                aggregated['cancellations'] += 1

            # Group by server if available
            server = reservation.get('server_name') or reservation.get('captain', '')
            if server:
                normalized_server = REVERSE_MERGE_MAP.get(server.lower(), server)
                if normalized_server not in aggregated['by_server']:
                    aggregated['by_server'][normalized_server] = {
                        'count': 0,
                        'covers': 0
                    }
                aggregated['by_server'][normalized_server]['count'] += 1
                aggregated['by_server'][normalized_server]['covers'] += party_size

            # Group by day
            res_date = reservation.get('date') or reservation.get('reservation_date', '')
            if res_date:
                day_str = res_date.split('T')[0] if 'T' in res_date else res_date
                if day_str not in aggregated['by_day']:
                    aggregated['by_day'][day_str] = {
                        'count': 0,
                        'covers': 0
                    }
                aggregated['by_day'][day_str]['count'] += 1
                aggregated['by_day'][day_str]['covers'] += party_size

        return aggregated

    def aggregate_reviews(self, reviews: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Aggregate review data by server."""
        aggregated = {}

        for review in reviews:
            server = review.get('server_name') or review.get('server', '')
            if not server:
                continue

            normalized_server = REVERSE_MERGE_MAP.get(server.lower(), server)

            if normalized_server not in aggregated:
                aggregated[normalized_server] = {
                    'overall_rating': [],
                    'food_rating': [],
                    'drink_rating': [],
                    'service_rating': [],
                    'ambience_rating': [],
                    'review_count': 0
                }

            aggregated[normalized_server]['review_count'] += 1

            if 'overall_rating' in review:
                aggregated[normalized_server]['overall_rating'].append(review['overall_rating'])
            if 'food_rating' in review:
                aggregated[normalized_server]['food_rating'].append(review['food_rating'])
            if 'drink_rating' in review:
                aggregated[normalized_server]['drink_rating'].append(review['drink_rating'])
            if 'service_rating' in review:
                aggregated[normalized_server]['service_rating'].append(review['service_rating'])
            if 'ambience_rating' in review:
                aggregated[normalized_server]['ambience_rating'].append(review['ambience_rating'])

        # Calculate averages
        for server in aggregated:
            for rating_type in ['overall_rating', 'food_rating', 'drink_rating', 'service_rating', 'ambience_rating']:
                ratings = aggregated[server][rating_type]
                if ratings:
                    avg = round(sum(ratings) / len(ratings), 2)
                    aggregated[server][f'{rating_type}_avg'] = avg
                aggregated[server].pop(rating_type)  # Remove list

        return aggregated


class TripleseatAPIClient:
    """Client for Tripleseat API integration (OAuth 1.0a)."""

    def __init__(self, api_token: str, consumer_key: str, consumer_secret: str, dry_run: bool = False):
        self.api_token = api_token
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.dry_run = dry_run
        self.api_base = "https://api.tripleseat.com/v1"

    def _sign_request(self, method: str, url: str, params: Dict[str, str]) -> str:
        """
        Generate OAuth 1.0a signature for Tripleseat API.

        Returns the Authorization header value.
        """
        import random
        import string

        # OAuth parameters
        nonce = ''.join(random.choices(string.ascii_letters + string.digits, k=32))
        timestamp = str(int(time.time()))

        oauth_params = {
            'oauth_consumer_key': self.consumer_key,
            'oauth_nonce': nonce,
            'oauth_signature_method': 'HMAC-SHA1',
            'oauth_timestamp': timestamp,
            'oauth_version': '1.0'
        }

        # Combine all params for signature base string
        all_params = {**oauth_params, **params}
        param_string = '&'.join(
            f"{k}={requests.utils.quote(str(v), safe='')}"
            for k, v in sorted(all_params.items())
        )

        # Create signature base string
        base_string = f"{method}&{requests.utils.quote(url, safe='')}&{requests.utils.quote(param_string, safe='')}"

        # Create signing key
        signing_key = f"{requests.utils.quote(self.consumer_secret, safe='')}&{requests.utils.quote(self.api_token, safe='')}"

        # Generate signature
        signature = base64.b64encode(
            hmac.new(
                signing_key.encode(),
                base_string.encode(),
                hashlib.sha1
            ).digest()
        ).decode()

        oauth_params['oauth_signature'] = signature

        # Build Authorization header
        auth_header = 'OAuth ' + ', '.join(
            f'{k}="{requests.utils.quote(str(v), safe="")}"'
            for k, v in sorted(oauth_params.items())
        )

        return auth_header

    def get_events(self, days_back: int = 30) -> List[Dict[str, Any]]:
        """
        Pull event data from Tripleseat API.

        Returns a list of events with guest counts and revenue information.
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Tripleseat: Would fetch events from last {days_back} days")
            return []

        events = []
        try:
            url = f"{self.api_base}/events.json"

            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

            params = {
                'start_date': start_date,
                'limit': 500,
                'offset': 0
            }

            logger.info(f"Tripleseat: Fetching events from {start_date}...")

            while True:
                # Generate OAuth signature
                auth_header = self._sign_request('GET', url, params)

                headers = {
                    'Authorization': auth_header,
                    'Content-Type': 'application/json'
                }

                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=10
                )
                response.raise_for_status()
                data = response.json()

                if 'events' not in data or not data['events']:
                    break

                events.extend(data['events'])
                logger.info(f"Tripleseat: Fetched {len(data['events'])} events (offset {params['offset']})")

                if len(data['events']) < params['limit']:
                    break

                params['offset'] += params['limit']
                time.sleep(0.5)  # Rate limiting

            logger.info(f"Tripleseat: Total events fetched: {len(events)}")
            return events

        except requests.exceptions.RequestException as e:
            logger.error(f"Tripleseat: Failed to fetch events: {e}")
            return events

    def aggregate_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate event data."""
        aggregated = {
            'total_events': len(events),
            'total_guests': 0,
            'total_revenue': 0.0,
            'by_day': {}
        }

        for event in events:
            # Count guests
            guest_count = event.get('guest_count') or event.get('guests', 0)
            aggregated['total_guests'] += guest_count

            # Sum revenue
            revenue = event.get('revenue') or event.get('total_revenue', 0)
            try:
                aggregated['total_revenue'] += float(revenue)
            except (ValueError, TypeError):
                pass

            # Group by day
            event_date = event.get('date') or event.get('event_date', '')
            if event_date:
                day_str = event_date.split('T')[0] if 'T' in event_date else event_date
                if day_str not in aggregated['by_day']:
                    aggregated['by_day'][day_str] = {
                        'count': 0,
                        'guests': 0,
                        'revenue': 0.0
                    }
                aggregated['by_day'][day_str]['count'] += 1
                aggregated['by_day'][day_str]['guests'] += guest_count
                try:
                    aggregated['by_day'][day_str]['revenue'] += float(revenue)
                except (ValueError, TypeError):
                    pass

        aggregated['total_revenue'] = round(aggregated['total_revenue'], 2)
        for day in aggregated['by_day']:
            aggregated['by_day'][day]['revenue'] = round(aggregated['by_day'][day]['revenue'], 2)

        return aggregated


class DashboardDataPipeline:
    """Main pipeline orchestrator."""

    def __init__(self, dry_run: bool = False, output_file: str = 'dashboard_data.json'):
        self.dry_run = dry_run
        self.output_file = output_file
        self.data = {
            'timestamp': datetime.now().isoformat(),
            'toast': {},
            'sevenrooms': {},
            'tripleseat': {},
            'errors': []
        }

    def run(self) -> bool:
        """Execute the full pipeline."""
        logger.info("=" * 60)
        logger.info("Starting Gigante Intelligence Dashboard Data Pipeline")
        logger.info("=" * 60)

        if self.dry_run:
            logger.info("DRY RUN MODE - No actual API calls will be made")

        # Initialize API clients
        toast_client = self._init_toast()
        sevenrooms_client = self._init_sevenrooms()
        tripleseat_client = self._init_tripleseat()

        # Fetch and process Toast data
        if toast_client:
            self._process_toast(toast_client)

        # Fetch and process SevenRooms data
        if sevenrooms_client:
            self._process_sevenrooms(sevenrooms_client)

        # Fetch and process Tripleseat data
        if tripleseat_client:
            self._process_tripleseat(tripleseat_client)

        # Write output
        self._write_output()

        logger.info("=" * 60)
        logger.info("Pipeline completed successfully")
        logger.info(f"Output written to {self.output_file}")
        logger.info("=" * 60)

        return True

    def _init_toast(self) -> Optional[ToastAPIClient]:
        """Initialize Toast API client."""
        client_id = os.getenv('TOAST_CLIENT_ID')
        client_secret = os.getenv('TOAST_CLIENT_SECRET')
        restaurant_guid = os.getenv('TOAST_RESTAURANT_GUID')

        if not all([client_id, client_secret, restaurant_guid]):
            logger.warning("Toast: Missing required environment variables, skipping")
            self.data['errors'].append("Toast: Missing TOAST_CLIENT_ID, TOAST_CLIENT_SECRET, or TOAST_RESTAURANT_GUID")
            return None

        return ToastAPIClient(client_id, client_secret, restaurant_guid, self.dry_run)

    def _init_sevenrooms(self) -> Optional[SevenRoomsAPIClient]:
        """Initialize SevenRooms API client."""
        venue_id = os.getenv('SEVENROOMS_VENUE_ID')
        client_secret = os.getenv('SEVENROOMS_CLIENT_SECRET')

        if not all([venue_id, client_secret]):
            logger.warning("SevenRooms: Missing required environment variables, skipping")
            self.data['errors'].append("SevenRooms: Missing SEVENROOMS_VENUE_ID or SEVENROOMS_CLIENT_SECRET")
            return None

        return SevenRoomsAPIClient(venue_id, client_secret, self.dry_run)

    def _init_tripleseat(self) -> Optional[TripleseatAPIClient]:
        """Initialize Tripleseat API client."""
        api_token = os.getenv('TRIPLESEAT_API_TOKEN')
        consumer_key = os.getenv('TRIPLESEAT_CONSUMER_KEY')
        consumer_secret = os.getenv('TRIPLESEAT_CONSUMER_SECRET')

        if not all([api_token, consumer_key, consumer_secret]):
            logger.warning("Tripleseat: Missing required environment variables, skipping")
            self.data['errors'].append("Tripleseat: Missing TRIPLESEAT_API_TOKEN, TRIPLESEAT_CONSUMER_KEY, or TRIPLESEAT_CONSUMER_SECRET")
            return None

        return TripleseatAPIClient(api_token, consumer_key, consumer_secret, self.dry_run)

    def _process_toast(self, client: ToastAPIClient):
        """Process Toast API data."""
        try:
            if not client.authenticate():
                logger.error("Toast: Authentication failed")
                self.data['errors'].append("Toast: Authentication failed")
                return

            feedbacks = client.get_feedbacks(days_back=30)
            if feedbacks:
                aggregated = client.aggregate_feedback_by_server(feedbacks)
                self.data['toast']['feedback'] = aggregated
                logger.info(f"Toast: Processed feedback for {len(aggregated)} servers")
            else:
                logger.warning("Toast: No feedback data retrieved")

        except Exception as e:
            logger.error(f"Toast: Unexpected error: {e}")
            self.data['errors'].append(f"Toast: {str(e)}")

    def _process_sevenrooms(self, client: SevenRoomsAPIClient):
        """Process SevenRooms API data."""
        try:
            if not client.authenticate():
                logger.error("SevenRooms: Authentication failed")
                self.data['errors'].append("SevenRooms: Authentication failed")
                return

            reservations = client.get_reservations(days_back=30)
            if reservations:
                aggregated = client.aggregate_reservations(reservations)
                self.data['sevenrooms']['reservations'] = aggregated
                logger.info(f"SevenRooms: Processed {aggregated['total_reservations']} reservations")

            reviews = client.get_reviews()
            if reviews:
                aggregated_reviews = client.aggregate_reviews(reviews)
                self.data['sevenrooms']['reviews'] = aggregated_reviews
                logger.info(f"SevenRooms: Processed reviews for {len(aggregated_reviews)} servers")
            else:
                logger.info("SevenRooms: No review data available")

        except Exception as e:
            logger.error(f"SevenRooms: Unexpected error: {e}")
            self.data['errors'].append(f"SevenRooms: {str(e)}")

    def _process_tripleseat(self, client: TripleseatAPIClient):
        """Process Tripleseat API data."""
        try:
            events = client.get_events(days_back=30)
            if events:
                aggregated = client.aggregate_events(events)
                self.data['tripleseat']['events'] = aggregated
                logger.info(f"Tripleseat: Processed {aggregated['total_events']} events")
            else:
                logger.warning("Tripleseat: No event data retrieved")

        except Exception as e:
            logger.error(f"Tripleseat: Unexpected error: {e}")
            self.data['errors'].append(f"Tripleseat: {str(e)}")

    def _write_output(self):
        """Write aggregated data to JSON file."""
        try:
            output_path = os.path.abspath(self.output_file)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            with open(output_path, 'w') as f:
                json.dump(self.data, f, indent=2)

            logger.info(f"Output written to {output_path}")

            # Print data summary
            logger.info("\n" + "=" * 60)
            logger.info("Data Summary:")
            logger.info(f"- Toast feedback servers: {len(self.data['toast'].get('feedback', {}))}")
            logger.info(f"- SevenRooms reservations: {self.data['sevenrooms'].get('reservations', {}).get('total_reservations', 0)}")
            logger.info(f"- SevenRooms review servers: {len(self.data['sevenrooms'].get('reviews', {}))}")
            logger.info(f"- Tripleseat events: {self.data['tripleseat'].get('events', {}).get('total_events', 0)}")
            logger.info(f"- Errors: {len(self.data['errors'])}")
            logger.info("=" * 60 + "\n")

        except Exception as e:
            logger.error(f"Failed to write output: {e}")
            raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Gigante Intelligence Dashboard Data Pipeline'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print what would be done without making API calls'
    )
    parser.add_argument(
        '--output',
        default='dashboard_data.json',
        help='Output JSON file path (default: dashboard_data.json)'
    )

    args = parser.parse_args()

    pipeline = DashboardDataPipeline(dry_run=args.dry_run, output_file=args.output)

    try:
        success = pipeline.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
