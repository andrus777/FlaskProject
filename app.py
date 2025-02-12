# https://proglib.io/p/samouchitel-po-python-dlya-nachinayushchih-chast-23-osnovy-veb-razrabotki-na-flask-2023-06-27

from flask import Flask, request, render_template
from models import Artist, Album, Song, db


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///music.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# связываем приложение и экземпляр SQLAlchemy
db.init_app(app)

@app.route('/songs')
def songs():
    songs_list = Song.query.all()
    return render_template('songs.html', songs=songs_list)

@app.route('/')
def hello_world():  # put application's code here
    return render_template('base.html')
@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/blog')
def blog():
    return 'Это блог с заметками о работе и увлечениях.'

@app.route('/user/<username>')
def user_profile(username):
    return f"Это профиль пользователя {username}"

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        # проверка логина и пароля
        return 'Вы вошли в систему!'
    else:
        return render_template('login.html')

if __name__ == '__main__':
    # Debug/Development
    app.run(debug=True, host="192.168.85.73", port="5000")
    # Production
    # http_server = WSGIServer(('', 5000), app)
    # http_server.serve_forever()
