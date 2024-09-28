import sqlite3

class UserDB:
    def __init__(self, db_name):
        self.db_name = db_name

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        # Исправлен синтаксис SQL-запроса (убрана лишняя запятая)
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            id INTEGER
        )''')
        self.conn.commit()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def get_user_data(self, id):
        try:
            res = self.cursor.execute('''SELECT * FROM users WHERE id = ?''', (id,)).fetchone()
            if res:
                return res
            else:
                return None
        except sqlite3.Error as e:
            print("Error fetching user data:", e)
            return None

    def get_all_data(self):
        self.cursor.execute("SELECT * FROM users")          
        users = self.cursor.fetchall()
        return users

    def add_user(self, id: int):
        if not self.get_user_data(id):
            try:
                self.cursor.execute('''INSERT INTO users (id) VALUES (?)''', (id,))
                self.conn.commit()
                return True
            except sqlite3.IntegrityError as e:
                print("Error:", e)
                return False
