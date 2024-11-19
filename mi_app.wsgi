import sys
import logging
from myapp import app as application

sys.stdout = sys.stderr
logging.basicConfig(stream=sys.stderr)

# Activar el entorno virtual
activate_this = '/home/ubuntu/myApp/venv/bin/activate.py'
exec(open(activate_this).read(), {'__file__': activate_this})
