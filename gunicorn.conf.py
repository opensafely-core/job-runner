# Where to log to (stdout and stderr)
accesslog = "-"
errorlog = "-"

# Configure log structure
# # http://docs.gunicorn.org/en/stable/settings.html#logconfig-dict
# logconfig_dict = logging_config_dict

# workers
workers = 5

# listen
port = 5000
bind = "0.0.0.0"
