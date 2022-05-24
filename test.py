from abc import abstractclassmethod


class Avensers(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        print(attrs)
        new = super().__new__(cls, name, bases, attrs)
        print(attrs.pop('Meta', "123"))
        print(getattr(new, "Meta", 'abc'))
        return new

    def __init__(cls, *args, **kwargs):
        print('Avensers __init__')
        super().__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        print('Avensers __call__')
        return super().__call__(*args, **kwargs)
    
class c(metaclass=Avensers):
    pass

class Tor(c, metaclass=Avensers):
    def __init__(self):
        print('Tor __init__')

    def __call__(self):
        print('Tor __call__')
    def __class__(self):
        pass
    class Meta:
        pass

a = Tor()
getattr(a, 'Meta')