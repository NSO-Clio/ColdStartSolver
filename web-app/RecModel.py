import pandas as pd
import numpy as np
import json
import pickle
import os
import random
import pyarrow.parquet as pq
from rectools.dataset import Dataset
from implicit.nearest_neighbours import TFIDFRecommender, CosineRecommender, BM25Recommender
from rectools.models import ImplicitItemKNNWrapperModel, PopularModel, PureSVDModel
import numpy as np
from datetime import datetime
from tqdm import tqdm


class Kyuriy:
    def __init__(self) -> None:
        """Инициализация класса и загрузка обученных моделей RecTools из файлов."""

        model_PureSVDModel_path = os.path.abspath('models_and_dataset/PureSVDModel.pkl')
        model_ImplicitItemKNNWrapperModel_path = os.path.abspath('models_and_dataset/ImplicitItemKNNWrapperModel.pkl')
        model_PopularModel_path = os.path.abspath('models_and_dataset/PopularModel.pkl')

        with open(model_PureSVDModel_path, "rb") as f:
            self.model_PureSVDModel: PureSVDModel = pickle.load(f)

        with open(model_ImplicitItemKNNWrapperModel_path, "rb") as f:
            self.model_ImplicitItemKNNWrapperModel: ImplicitItemKNNWrapperModel = pickle.load(f)

        with open(model_PopularModel_path, "rb") as f:
            self.model_PopularModel: PopularModel = pickle.load(f)

        self.dataset = None
        self.data_users: list[str] = []
        self.video_stat_info: pd.DataFrame = pd.DataFrame()
    
    def update_dataset(self, interactions_new_user: pd.DataFrame) -> None:
        """
        Обновление набора данных новыми взаимодействиями пользователей.

        :param interactions_new_user: DataFrame с колонками [datetime, user_id, item_id, weight]
        :return: Обновленный объект Dataset.
        :raises ValueError: Если dataset или data_users не инициализированы.
        """

        if self.dataset is None or not self.data_users:
            raise ValueError("Необходимо запустить метод create_data для инициализации данных.")

        # past_dataset это Dataset.construct, interactions_new_user датафрейм похожий на изначальный, но в нем находятся только размеченые пользователем рекомендации
        r = self.dataset.item_features
        sparse_coo = r.values.tocoo()
        df_item_features = pd.DataFrame({
            'item_id': sparse_coo.row,
            'feature': sparse_coo.col,
            'value': sparse_coo.data
        })
        feat = np.array(self.dataset.item_features.names)[:, 0]
        dct = {i: feat[i] for i in range(len(feat))}
        df_item_features['item_id'] = self.dataset.item_id_map.convert_to_external(df_item_features['item_id'])
        df_item_features['feature'] = df_item_features['feature'].map(dct)
        
        # Удаление временной зоны перед преобразованием
        interactions_new_user['datetime'] = interactions_new_user['datetime'].dt.tz_localize(None)
        # Теперь можно безопасно преобразовать в datetime64[ns]
        interactions_new_user['datetime'] = interactions_new_user['datetime'].astype("datetime64[ns]")
        
        interactions_new = pd.concat([self.dataset.get_raw_interactions(), interactions_new_user], ignore_index=True)
        self.dataset = Dataset.construct(
            interactions_df=interactions_new,
            item_features_df=df_item_features,
            cat_item_features=['category_id', 'author_id']
        )
    
    def create_data(self, video_stat_path: str, logs_df_path: str) -> None:
        """
        Инициализация базы данных для вывода информации о видео.

        :param video_stat_path: Путь к файлу с статистикой видео в формате .parquet.
        :param logs_df_path: Путь к файлу с логами в формате .parquet.
        """
        
        video_stat = pd.DataFrame()
        parquet_file = pq.ParquetFile(os.path.abspath(video_stat_path))
        for _, batch in tqdm(enumerate(parquet_file.iter_batches(batch_size=100000)), total=parquet_file.num_row_groups):
            d = batch.to_pandas()
            video_stat = pd.concat([video_stat, d], ignore_index=True)
        video_stat = self.__reduce_mem_usage(video_stat)
        video_stat.drop(columns='row_number', inplace=True)
        video_stat.reset_index(drop=True, inplace=True)
        # Переименование колонки для унификации
        video_stat.rename(columns={'video_id': 'item_id'}, inplace=True)
        self.video_stat_info = video_stat[['item_id', 'title', 'category_id', 'description']].copy()
        print("video_stat -> Done, 1/5 point")

        logs_df = pd.DataFrame()
        parquet_file = pq.ParquetFile(os.path.abspath(logs_df_path))
        for _, batch in tqdm(enumerate(parquet_file.iter_batches(batch_size=100000)), total=parquet_file.num_row_groups):
            d = batch.to_pandas().drop(columns=['region', 'city']).rename(columns={'event_timestamp': 'datetime'})
            d['datetime'] = pd.to_datetime(d['datetime'])
            d = self.__reduce_mem_usage(d)
            logs_df = pd.concat([logs_df, d], ignore_index=True)
        logs_df = self.__reduce_mem_usage(logs_df)
        # Переименование колонки для унификации
        logs_df.rename(columns={'video_id': 'item_id'}, inplace=True)
        print("logs_df -> Done, 2/5 point")

        logs_df = logs_df.merge(video_stat[['v_duration', 'item_id']])
        # Вычисление веса взаимодействий
        logs_df['weight'] = logs_df['watchtime'] / logs_df['v_duration'] * 5
        logs_df.replace(np.inf, np.nan)
        # Удаление NaN значений и корректировка веса
        logs_df.dropna(inplace=True)
        logs_df['weight'] = logs_df['weight'].apply(lambda x: 5 if x > 5 else 0 if x < 0 else x)
        # Округление веса до целого числа
        logs_df['weight'] = logs_df['weight'].apply(round)
        
        # Удаление ненужных колонок
        logs_df.drop(columns=['v_duration', 'watchtime'], inplace=True)
        cols = video_stat.drop(columns=['title', 'description', 'v_pub_datetime']).columns.tolist()
        # Преобразуем видео статистику в "длинный" формат (feature-value)
        item_features_df = video_stat.melt(id_vars=['item_id'], value_vars=cols,
                                            var_name='feature', value_name='value')
        item_features_df = self.__reduce_mem_usage(item_features_df)
        print("item_features_df -> Done, 3/5 point")

        self.dataset = Dataset.construct(
            interactions_df=logs_df,
            item_features_df=item_features_df,
            cat_item_features=['category_id', 'author_id']
        )
        print("dataset -> Done, 4/5 point")

        self.data_users = self.dataset.get_raw_interactions()['user_id'].unique().tolist()
        print("data_users -> Done, 5/5 point")
        print("create data -> Done")
    
    def recommend(self, user_id: str, get_rec_type: str) -> dict | pd.DataFrame:
        """
        Генерация рекомендаций для пользователя.

        :param user_id: Идентификатор пользователя.
        :param get_rec_type: Тип возвращаемых данных ('json' или 'DataFrame').
        :return: Рекомендации в формате json или DataFrame.
        :raises ValueError: Если dataset или data_users не инициализированы.
        """

        if self.dataset is None or not self.data_users:
            raise ValueError("Необходимо запустить метод create_data для инициализации данных.")

        if user_id in self.data_users:
            recommendations = self.model_ImplicitItemKNNWrapperModel.recommend(
                users=[user_id],
                dataset=self.dataset,
                k=10,
                filter_viewed=True
            )
            if len(recommendations) < 10:
                rec2 = recommendations = self.model_PureSVDModel.recommend(
                    users=[user_id],
                    dataset=self.dataset,
                    k=10,
                    filter_viewed=True
                ).iloc[:10 - len(recommendations), :]
                recommendations = pd.concat([recommendations, rec2])
        else:
            recommendations = self.model_PopularModel.recommend(
                users=[user_id],
                dataset=self.dataset,
                k=10,
                filter_viewed=True
            )
        if get_rec_type == 'json':
            json_req = {}
            for i in self.video_stat_info[self.video_stat_info['item_id'].isin(recommendations['item_id'])].values:
                json_req[i[0]] = i[1:].tolist()
            return json_req
        elif get_rec_type == 'DataFrame':
            return self.video_stat_info[self.video_stat_info['item_id'].isin(recommendations['item_id'])]
    
    def fit_models(self) -> None:
        """Обучение моделей на основе текущего набора данных."""

        if self.dataset is None or not self.data_users:
            raise ValueError("Необходимо запустить метод create_data для инициализации данных.")

        self.model_ImplicitItemKNNWrapperModel.fit(self.dataset)
        self.model_ImplicitItemKNNWrapperModel.fit(self.dataset)
        self.model_ImplicitItemKNNWrapperModel.fit(self.dataset)


    @staticmethod
    def __reduce_mem_usage(df: pd.DataFrame) -> pd.DataFrame:
        """
        Оптимизация использования памяти путем изменения типов данных в DataFrame.

        :param df: Исходный DataFrame.
        :return: Оптимизированный DataFrame.
        """

        for col in df.columns:
            col_type = df[col].dtype.name

            # Пропускаем столбцы с типом 'object', 'category', 'datetime'
            if col_type in ['object', 'category']:
                continue  # Пропускаем эти столбцы

            if col_type.startswith('datetime'):
                continue  # Пропускаем столбцы с временными метками

            c_min = df[col].min()
            c_max = df[col].max()

            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:  # Для столбцов с вещественными типами
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        return df
