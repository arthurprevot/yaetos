from ConfigParser import ConfigParser
import os

def get_user_pass(cred_profile, fpath=None):
    config = ConfigParser()
    if not fpath:
        fpath = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'api_creds.cfg')
    config.read(fpath)
    user = config.get(cred_profile, 'user')
    password = config.get(cred_profile, 'password')
    return user, password


def get_token(cred_profile, fpath=None):
    config = ConfigParser()
    if not fpath:
        fpath = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'api_creds.cfg')
    config.read(fpath)
    return config.get(cred_profile, 'token')
