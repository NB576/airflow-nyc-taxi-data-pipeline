from pendulum import datetime

S3_BUCKET = "nyc-taxi-project-112025"

START_DATE = datetime(2024, 7, 1)
END_DATE = datetime(2024, 12, 1)

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "none"
}

PAYMENT_MAP = { 0: "Flex Fare", 1: "Credit", 2: "Cash", 3: "No Charge", 4: "Dispute", 5: "Unknown", 6: "Voided" }
RATECODE_MAP = { 1: "Standard", 2: "JFK", 3: "Newark", 4: "Nassau/Westchester", 5: "Negotiated", 6: "Group ride", 99: "Null/Unknown" }
