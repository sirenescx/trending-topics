from pathlib import Path

from flask import Flask, render_template, send_from_directory
from wordcloud import WordCloud

app = Flask(__name__, template_folder='templates')


@app.route('/')
def index():
    data = {'word1': 10, 'word2': 8, 'word3': 15}
    wordcloud = WordCloud(width=1920, height=1080, background_color='white').generate_from_frequencies(data)
    image_path = Path(__file__).parent.resolve() / 'static/wordcloud.png'
    wordcloud.to_file(image_path)

    return render_template('index.html', image_path=image_path)


@app.route('/static/<filename>')
def serve_static(filename):
    return send_from_directory('static', filename)


if __name__ == '__main__':
    app.run(debug=True)
