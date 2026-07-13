import os
import sys
import logging


def load_config(proj_home=None):
    """
    Load configuration from config.py, optionally overridden by local_config.py.
    Both files are looked up in proj_home. Uppercase-named variables are extracted.
    """
    if proj_home is None:
        proj_home = os.path.realpath(os.path.dirname(__file__))

    config = {'PROJ_HOME': proj_home}

    added_to_path = proj_home not in sys.path
    if added_to_path:
        sys.path.insert(0, proj_home)

    try:
        for fname in ('config.py', 'local_config.py'):
            fpath = os.path.join(proj_home, fname)
            if os.path.exists(fpath):
                namespace = {}
                with open(fpath) as f:
                    exec(compile(f.read(), fpath, 'exec'), namespace)
                for key, value in namespace.items():
                    if key.isupper():
                        config[key] = value
    finally:
        if added_to_path and proj_home in sys.path:
            sys.path.remove(proj_home)

    return config


def setup_logging(name, proj_home=None, level='INFO', attach_stdout=False):
    """
    Return a logger for the given name, adding a handler the first time it is called.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout if attach_stdout else sys.stderr)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        logger.addHandler(handler)
    return logger