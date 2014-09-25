from importlib import import_module

def load_class(path):
    """
    Load class from path.
    """

    mod_name, klass_name = path.rsplit('.', 1)

    try:
        mod = import_module(mod_name)
    except AttributeError as e:
        raise RuntimeError('Error importing {0}: "{1}"'.format(mod_name, e)) from e

    try:
        klass = getattr(mod, klass_name)
    except AttributeError:
        raise RuntimeError('Module "{0}" does not define a "{1}" class'.format(mod_name, klass_name))

    return klass


def load_queue_implementation(appconf:dict):
    """
    Load queue implementation for
    current application configuration.
    """
    queue_conf = appconf["queue_conf"]
    queue_cls = load_class(queue_conf["path"])
    return queue_cls(**queue_conf["kwargs"])



