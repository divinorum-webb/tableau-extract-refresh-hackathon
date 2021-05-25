server_config = {
    'tableau': {
        'server': <YOUR_SERVER>,
        'api_version': '3.11',
#         'username': '<YOUR_USERNAME>',  # if you use username / password then do NOT use PAT credentials below
#         'password': '<YOUR_PASSWORD>',
        'personal_access_token_name': '<YOUR_PAT_NAME>',
        'personal_access_token_secret': '<YOUR_PAT_SECRET>',
        'site_name': 'YourSiteName!!',
        'site_url': 'YourSiteName'  # note that the site_url is your site name as it appears in your URL
    }
}
