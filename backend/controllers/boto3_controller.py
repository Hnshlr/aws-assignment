import boto3

# GET BOTO3 SESSION CREDENTIALS, SUCCESSFULLY AUTHENTICATED USING UPDATED LOCAL CREDENTIALS:
def get_boto3_session_credentials():
    session = boto3.session.Session()
    credentials = session.get_credentials()
    return credentials