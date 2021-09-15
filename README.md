# SGDI
prácticas para la asignatura SGDI

## Para empezar

1. Crear un fichero `Constants.py` que usaremos para guardar nuestro SECRET
2. Añadir el API key de AEMET OpenData en la clase creada, así:
```python
API_KEY_SERVICE="misecreto"
```
3. Añadir la clase Constants como import en el notebook para luego poder
usar nuestro SECRET en el código, así:

```python
...
import Constants

api_key = Constants.API_KEY_SERVICE
```

De esta manera, nos evitamos compartir nuestra key y cada uno puede trabajar
de manera independiente.