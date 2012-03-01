# largely based on Django's config mechanism (django.conf.__init__)

import os

from caa.utils import LazyObject, importlib

ENVIRONMENT_VARIABLE = "CAA_SETTINGS_MODULE"

class LazySettings(LazyObject):
    """
    A lazy proxy. The user can manually configure settings prior to using them. Otherwise,
    the settings module pointed to by the value of ENVIRONMENT_VARIABLE will be used.
    """
    def _setup(self):
        """
        Load the settings module pointed to by the environment variable. This
        is used the first time we need any settings at all, if the user has not
        previously configured the settings manually.
        """
        try:
            settings_module = os.environ[ENVIRONMENT_VARIABLE]
            if not settings_module: # If it's set but is an empty string.
                raise KeyError
        except KeyError:
            # NOTE: This is arguably an EnvironmentError, but that causes
            # problems with Python's interactive help.
            raise ImportError("Settings cannot be imported, because environment variable %s is undefined." % ENVIRONMENT_VARIABLE)

        self._wrapped = Settings(settings_module)

class Settings(object):
    def __init__(self, settings_module):
        # store the settings module in case someone later cares
        self.SETTINGS_MODULE = settings_module

        try:
            mod = importlib.import_module(self.SETTINGS_MODULE)
        except ImportError, e:
            raise ImportError("Could not import settings '%s' (Is it on sys.path?): %s" % (self.SETTINGS_MODULE, e))

        for setting in dir(mod):
            if setting == setting.upper():
                setting_value = getattr(mod, setting)
                setattr(self, setting, setting_value)

settings = LazySettings()



