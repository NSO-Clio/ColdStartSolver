from flask import Flask, render_template, request, redirect, url_for, jsonify
import pandas as pd
import os
from db import UserDB
from RecModel import Kyuriy


recModel: Kyuriy = None
app = Flask(__name__)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/enter', methods=['POST'])
def enter():
    user_id = request.form['id']
    print(f"Получен ID пользователя: {user_id}")
    with UserDB('user_database.db') as db:
        db.add_user(user_id)
    return redirect(url_for('videos', user_id=user_id))


@app.route('/videos', methods=['GET'])
def videos():
    user_id = request.args.get('user_id')
    print(user_id)
    return render_template('videos.html', user_id=user_id)


@app.route('/get_videos', methods=['GET'])
def get_videos():
    user_id = request.args.get('user_id')
    try:
        user_data = recModel.recommend(user_id=user_id, get_rec_type='json')
        
        return jsonify(user_data), 200
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        return jsonify({"error": "Произошла ошибка на сервере"}), 500


@app.route('/regenerate', methods=['POST'])
def regenerate():
    data = request.get_json()
    user_id = data.get('user_id')
    print(data)
    print(user_id)
    new_data_rec = pd.DataFrame({
        'datetime': [pd.Timestamp.now()] * 10,
        'user_id': [user_id] * 10,
        'item_id': [i for i in data.keys() if i != 'user_id'],
        'weight': [round(float(i)) for i in data.values()][:-1]
    })
    print(new_data_rec)
    print('Start update_dataset')
    recModel.update_dataset(
        interactions_new_user=new_data_rec
    )
    print('update_dataset Done')
    return jsonify({"redirect_url": url_for('videos', user_id=user_id)}), 200


if __name__ == '__main__':
    recModel = Kyuriy()
    recModel.create_data(
        video_stat_path='models_and_dataset/video_stat.parquet',
        logs_df_path='models_and_dataset/logs_df_2024-08-06.parquet'
    )
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)