# SGDI
prácticas para la asignatura SGDI

## Para empezar

1. Crear un fichero `Constants.py` que usaremos para guardar nuestro SECRET
y la ruta al comando chromadriver
2. Dentro del fichero, añadir el siguiente código:
```python
# Straightforward implementation of the Singleton Pattern
# Taken from https://python-patterns.guide/gang-of-four/singleton/
class Const(object):
  _instance = None
  api_key = ""
  chromedriver = ""

  def __new__(cls):
    if cls._instance is None:
      print('Creating the object')
      cls._instance = super(Const, cls).__new__(cls)
      # Put any initialization here.
      cls.api_key = "mysecret"
      cls.chromedriver = "/path/to/chromedriver"
    return cls._instance
```

3. Añadir la clase Constants como import en el notebook para luego poder
usar nuestro SECRET en el código, así:

```python
...
from constants import Const
...
const = Const()
api_key = const.api_key
chromedriver = const.chromedriver
```

4. Una vez acabada la configuración, ya podemos empezar a trabajar en nuestro
playbook!

De esta manera, nos evitamos compartir nuestra key y cada uno puede trabajar
de manera independiente.