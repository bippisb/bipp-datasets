from playwright.sync_api import sync_playwright

# Function to extract the authorization header from a response


def extract_authorization_header(response):
    headers = response.headers
    return headers.get("authorization")

# Function to monitor network requests and extract the authorization header


def get_auth_token():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()

        # Enable network request interception
        page.route("**/*", lambda route, request: route.continue_())

        # Intercept network responses and extract the authorization header
        def handle_response(response):
            if response.url == "https://cimsdbie.rbi.org.in/CIMS_Gateway_DBIE/GATEWAY/SERVICES/dbie_getBankANDBankGrp":
                authorization_header = extract_authorization_header(response)
                if authorization_header:
                    page._authorization_header = authorization_header

        page.on("response", handle_response)

        # Navigate to the website's page where the async request occurs
        page.goto("https://cimsdbie.rbi.org.in/#/banking-outlet")


        # Close the browser
        browser.close()

        # Return the extracted authorization header as the token
        return page._authorization_header


if __name__ == "__main__":
    auth_token = get_auth_token()
    if auth_token:
        print("Authorization Token:", auth_token)
    else:
        print("Authorization Token not found.")
