const thankElements = document.getElementsByClassName('rec_thank');
const buttons = document.getElementsByClassName('rec_btn');

const sub_btn = document.getElementsByClassName('submiton_button');
const checkboxes = document.querySelectorAll('input[type="radio"]');

let selectedRadioValues = {}; // Храним данные выборов пользователя

document.addEventListener('DOMContentLoaded', async function() {
    try {
        const response = await fetch(`/get_videos?user_id=${user_id}`);  // Передаем user_id в запросе

        if (response.ok) {
            const data = await response.json();
            renderVideos(data);  // Выводим видео на страницу
        } else {
            console.error('Ошибка при получении данных:', response.statusText);
        }
    } catch (error) {
        console.error('Произошла ошибка при получении данных:', error);
    }
});

// Функция для отрисовки блоков видео
function renderVideos(videos) {
    const container = document.getElementById('rec_container');
    container.innerHTML = '';  // Очищаем контейнер

    Object.entries(videos).forEach(([id, videoData]) => {
        const videoBlock = document.createElement('div');
        videoBlock.className = 'rec_block';
        videoBlock.id = id;

        // Генерация HTML для каждого блока видео
        videoBlock.innerHTML = `
            <h2 class="rec_h2">${videoData[0]}</h2>
            <ul class="rec_ul">
                Тематика видео: ${videoData.slice(1).map(point => `<li>${point}</li>`).join('')}
            </ul>
            <div class="rec_radio">
                <p>Как вам видео?</p>
                <input type="radio" id="like_${id}" name="${id}" value="5"/>
                <label for="like_${id}">Понравилось</label>

                <input type="radio" id="norm_${id}" name="${id}" value="2.5"/>
                <label for="norm_${id}">Сойдёт</label>

                <input type="radio" id="dislike_${id}" name="${id}" value="0"/>
                <label for="dislike_${id}">Не понравилось</label>
            </div>
        `;

        container.appendChild(videoBlock);
    });

    // Инициализация обработчиков для всех радиокнопок
    initializeRadioButtons();
}

// Функция для привязки событий к радиокнопкам
function initializeRadioButtons() {
    document.querySelectorAll('input[type="radio"]').forEach(radio => {
        radio.addEventListener('change', function () {
            // Сохраняем выбранное значение для каждого видео
            selectedRadioValues[this.name] = this.value;

            // Логируем объект, чтобы проверить текущее состояние всех групп радио-кнопок
            console.log('Текущее состояние радио-кнопок:', selectedRadioValues);
        });
    });
}

// Обработка нажатия на кнопку "Перестроить рекомендацию"
document.getElementById('submitBtn').addEventListener('click', function() {
    console.log('Отправляем данные на сервер:', selectedRadioValues);

    // Добавляем user_id в данные для отправки
    const dataToSend = {
        ...selectedRadioValues,
        user_id: user_id  // Включаем user_id
    };

    // Отправка данных на сервер
    fetch('/regenerate', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(dataToSend)  // Используем обновленные данные
    })
    .then(response => response.json())
    .then(data => {
        console.log('Успех:', data);

        // Проверяем наличие URL для перенаправления
        if (data.redirect_url) {
            window.location.href = data.redirect_url;  // Перенаправление на нужную страницу
        }

        // Сброс радио-кнопок после отправки
        selectedRadioValues = {};
        document.querySelectorAll('input[type="radio"]').forEach(radio => {
            radio.checked = false;
        });
    })
    .catch(error => {
        console.error('Ошибка:', error);
    });
});