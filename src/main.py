from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello_docker():
    return 'Hello Docker!'


# Only for testing purposes - should use WSGI server in production
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)