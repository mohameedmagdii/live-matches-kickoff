import streamlit as st
import pandas as pd
from datetime import timedelta, datetime
import gspread
import time
import requests


class Fotmob(object):
    def __init__(self, match_id):
        self.match_events_api = 'https://www.fotmob.com/api/data/matchDetails?matchId='
        self.match_id = match_id

    def request_send(self):
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        }
        response = requests.get(self.match_events_api + str(self.match_id), headers=headers)
        response.raise_for_status()
        response_json = response.json()
        return response_json

    def get_match_times(self):
        match_events = self.request_send()
        match_times = list(match_events['header']['status']['halfs'].values())
        return match_times


def get_credentials():
    """Returns Google Sheets credentials"""
    credentials = {
        "type": "service_account",
        "project_id": "second-sheets-api",
        "private_key_id": "ac206a8e52dbbffd8216c587d5806cfabd0fbcac",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDgYZGwn7zQJzVn\nvgfziwkwRcqXVMw5AY0XeXgbzw6kCu4Vejm62tzOyzfnrPw6quXn33AFdkzA0BKV\nUT29gFbZau03qrR1/43xBq67qzKycoe4BjIaX9xUjWjVIe1XH9tIpuXqEa2ZWoaK\nmA9gx6j6xQgIClARl4OaOX5sASVPDfsN7NiakQXyizp5JpsnOlYEKmLiqxWXkuAh\nEpmGcqrZ/drOGTbYBmqmnhWCnzbrvAYmOe35I9Ls0I/SNuQAEA49nLn6HCTAyZhP\nNoYBQjQvBYxpHAdSgBssRssgjk/yhOX6ZKkKSDNhvmu9CvY0bYbaHs+PiG2Xqh61\nA16YLllpAgMBAAECggEAHEVGzy9tIXfTM4E+rLmHnJiqhCMnwQX4Qi0ojC5sLJCA\n+11FQUz6laaGom7J79Vg9rRQ/6suU/vTX5NXGVV+e9HMVh9wsc5TUrsyEEyrbE7g\nWTgUn3yU/OU9QF9QMVI+9XorzTBRhFkiyvsKMvotCP2Cu8qa2ICT0P5t9os1aW5/\n1br0Zw2QZ7wNFxFGN/erBqYF9/LhNKPc8d8NB32YiBtqfG5mZ6xb5wuUjrw50Pyq\nugmT6FlLyXecOa0tffTkYbxqxVREIue+Hh1LTb8c6v9zfcrRgoLeOTEmY1z0qS3I\nzAhuNkAIMfhEJg8o5Kp4fIXY786wz9fUeX1pHVYh5QKBgQD32j0GwCd/29hoLAS7\nHCK1Jhape5BsrB5+iLC0hOZUfAv2+VV1Qw9IVLCDcTCsuncpszlZioCAW/NP9JG5\nR0c/506+D5pd9dHmc3tP8fD/wU3FLvMrkNs1/1V8CyqpdJ52TWG7tplQxXOOjzcD\nI7biaAL3ZY6FNhlrMD/s4ndhtQKBgQDnwc+zx6wA3fTfBj2DZhEIXn94wyI4RP8/\nwYISMRRqttUP3jFxPDX/h6Iy7uSy2KunJBEZMIZYkIbCrpvaXY+9/J53wAgXMadH\npXKaw6Yp4FkWZMl13MaMrOtejb1/GSZPn9P9FB6LXivXfNRkirjyhvKua9qPWNda\n6OI2IzW5ZQKBgFZQSyZZrqfJQPDuO2uJ3dBmBkhZfq9JtYjtQ1RqX+gWFviYVhlz\nFBRlYd80gPX2Ld94ycUUBbIt02sJyeHTH83yWKq5tlF4dPOjTdNGd14pzFKaChcm\n6CBC3ltHbED9Gt4qDpoXegb17Es+NrNCMcNg43+Sf3KGp417jReKunZtAoGAPo2g\nWx0EvwMpqdzMU6AD7udarqS0os10u6emTPS0Vw1cOrk1TSP8Syk3zVmPnvo+cpGY\nj9z6FgOEbB2m8WyZdKUvvJC3XlNHnF36re0q91iXkyTG/6oEaUnvCYlmYVzPa2Rv\nWnrIt8NGQBZwWWbb/pIQBKYVyuUm02HSVNxyDsUCgYEAhCdxfymJa5ckHl7eAP5X\n8uortGKScFVjlD3o8thfjXD69u/vZkkuaJAyS3OFrKmHiNeZZdXWCHVjJm7KtiI8\nljZju4gXKziebADcljPfAaD2AgOjvBssT0z0IAnWz5KAjRuj4U+FCec8fmzpdK9R\nRPSsOB0D1G0VEAvsKqAWpd8=\n-----END PRIVATE KEY-----\n",
        "client_email": "matches-ko-time@second-sheets-api.iam.gserviceaccount.com",
        "client_id": "105707912261771619160",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/matches-ko-time%40second-sheets-api.iam.gserviceaccount.com"
    }
    return credentials


def extract_match_info(matches_df, worksheet):
    """Extract match timing information from Fotmob and update Google Sheet"""

    all_matches_list = matches_df.OS_MatchID.tolist()
    all_matches_list_sb = matches_df.SB_MatchID.tolist()

    # Create progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()

    success_count = 0
    error_count = 0

    for idx, os_id in enumerate(all_matches_list):
        try:
            # Update progress
            progress = (idx + 1) / len(all_matches_list)
            progress_bar.progress(progress)
            status_text.text(f'Processing match {idx + 1} of {len(all_matches_list)}...')

            # Fetch match data
            match_json = Fotmob(os_id)
            times_list = match_json.get_match_times()

            # Convert timestamps and add one hour
            times_list = [
                datetime.strftime(
                    datetime.fromtimestamp(
                        time.mktime(datetime.strptime(date_str, '%d.%m.%Y %H:%M:%S').timetuple())
                    ) + timedelta(hours=1),
                    '%Y-%m-%d %I:%M:%S %p'
                ) if date_str else ''
                for date_str in times_list
            ]

            # Convert to strings
            times_list = [str(t) for t in times_list]

            # Update Google Sheet
            sb_id = all_matches_list_sb[all_matches_list.index(os_id)]
            row_num = worksheet.find(str(sb_id)).row
            worksheet.update(
                range_name=f'E{row_num}:L{row_num}',
                values=[times_list]
            )

            success_count += 1

        except Exception as e:
            error_count += 1
            st.warning(f'Error processing match {os_id}: {str(e)}')
            continue

    progress_bar.progress(1.0)
    status_text.text('Processing complete!')

    return success_count, error_count


def main():
    st.set_page_config(
        page_title="Match Info Validator",
        page_icon="⚽",
        layout="centered"
    )

    st.title("⚽ Validate Matches Info")
    st.markdown("---")

    # Date inputs
    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime(2026, 2, 1).date(),
            format="YYYY-MM-DD"
        )

    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime(2026, 2, 1).date(),
            format="YYYY-MM-DD"
        )

    st.markdown("---")

    # Fetch button
    if st.button("🚀 Start Fetching Data", type="primary", use_container_width=True):

        if start_date > end_date:
            st.error("❌ Start date must be before end date!")
            return

        try:
            with st.spinner("Connecting to Google Sheets..."):
                # Connect to Google Sheets
                credentials = get_credentials()
                worksheet = gspread.service_account_from_dict(credentials).open(
                    "Live Team's Schedule 2025"
                ).worksheet('Matches_KO_time')

                # Load data
                matches_df = pd.DataFrame(worksheet.get_all_records())
                matches_df = matches_df[['SB_MatchID', 'OS_MatchID', 'Match_name', 'Match_date', "Second_half_end"]]

                # Filter by date range and empty Second_half_end
                matches_df = matches_df[
                    (matches_df.Match_date >= str(start_date)) &
                    (matches_df.Match_date <= str(end_date)) &
                    (matches_df.Second_half_end == '')
                    ]

                if len(matches_df) == 0:
                    st.warning("⚠️ No matches found in the specified date range with missing data.")
                    return

                st.info(f"📊 Found {len(matches_df)} matches to process")

            # Process matches
            st.markdown("### Processing Matches")
            success_count, error_count = extract_match_info(matches_df, worksheet)

            # Show results
            st.markdown("---")
            st.success("✅ Processing Complete!")

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Successful Updates", success_count)
            with col2:
                st.metric("Errors", error_count)

        except Exception as e:
            st.error(f"❌ An error occurred: {str(e)}")
            st.exception(e)


if __name__ == "__main__":
    main()

