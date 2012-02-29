import json

class _Config(object):
    
    COMMENT_CHAR = '#'

    def __init__(self, fp):
        lines = ( line.partition(self.COMMENT_CHAR)[0] for line in fp)
        self._jsondata = json.loads(''.join(lines))
        
    @property
    def sections(self):
        return self._jsondata.keys()

    def __getattr__(self, attr):
        if attr not in self._jsondata:
            raise AttributeError("Section '%s' not found in config" % attr)

        return self._jsondata[attr]

def get_config(filename):
    with open(filename, 'r') as f:
        try: 
            res = _Config(f)
        except ValueError as e:
            msg = "Invalid configuration in file '%s'\n"\
                  "Details:\n" + str(e)
            raise ValueError(msg)
        return res
        




