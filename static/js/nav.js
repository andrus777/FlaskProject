// Функция для отправки запроса
function sendRequest(url, sect, dataSend = '') {
    const dataToSend = {"tbname": dataSend.toString()};
    //alert(JSON.stringify(dataToSend) + " ---- " + dataToSend);


    $(sect).html('<img src="/static/pre_loader_3.gif" alt="Изображение" style="text-align: center;">');
//alert(sect+ "  " + $(sect).html());
    $.ajax({
        type: 'POST',
        url: url,
        data: JSON.stringify(dataToSend),
        contentType: 'application/json',  // важно добавить этот заголовок!
        success: function (response) {
            // Вставляем полученный HTML в контейнер
            //alert(response);
            $(sect).html(response);
            //alert(url);
            if (url == "/eis/store") {
                //alert("store");
                initStoreHandlers();
                //initAdditionalHandlers();
            }
            if (url == "/eis/projects") {
                initAdditionalHandlers();
            }

            if (url == "/eis/project/") {
                $('#myModal').modal('show');
            }

            if (url == "adminp/update_table") {
                initStoreHandlers();
            }
            if (url == "eis/rez") {
                //alert("test");
                initModalHandlers();
            }
            if (url == "/demands/dem") {
                //alert("test");
                initDemHandlers();
            }
            if (url == "/eis/inprod") {
                initInProdHandlers();
            }
        },
        error: function (error) {
            console.error('Ошибка:', error);
            $(sect).html('Произошла ошибка');
        }
    });
}


// Проверка включен ли режим администрирования
function testADM(func_execute, url, sect, value) {
    const checkbox = document.getElementById('toggle_adm');
    //alert(checkbox.checked);
    if (checkbox.checked) {
        func_execute(url, sect, value);
    }
}

var exTable;

function initModalHandlers() {
    exTable = document.getElementById('exTableModal');
    //alert(exTable);
    var table = $('#exTableModal').DataTable({
        autoWidth: true,
        pageLength: 25,
        scrollCollapse: true,
        scrollY: '300px',
        //scrollX: true,
        paging: true,
        ordering: true,
        orderMulti: true,
        info: true,
        searching: true,
        language: {
            url: '/static/ru.json'
        }
    });

}

function initStoreHandlers() {
    $('#storeTable').DataTable({
        autoWidth: true,
        pageLength: 50,
        paging: true,
        ordering: true,
        info: true,
        orderMulti: true,
        searching: true,
        language: {
            url: '/static/ru.json'
        }
    });

}

function initInProdHandlers() {
    $('#inprodTable').DataTable({
        autoWidth: true,
        pageLength: 50,
        paging: true,
        ordering: true,
        info: true,
        orderMulti: true,
        searching: true,
        language: {
            url: '/static/ru.json'
        }
    });

}


function initDemHandlers() {
    const table = $('#ordersTable').DataTable({
        columns: [
            {data: 'Проект'},
            {data: 'БизнесРегион'},
            {data: 'Менеджер'},
            {data: 'Партнер'},
            {data: 'Номер'},
            {data: 'Дата'},
            {data: 'СтатусЗаказа'},
            {data: 'Соглашение'},
            {data: 'Количество'},
            {data: 'КоличествоРеал'},
            {data: 'СуммаРеал'},
            {data: 'Соглашение'},
            {data: 'Количество'},
            {data: 'КоличествоРеал'},
            {data: 'ДатаРеализации'},
            {data: 'ДокументРеализации'},
            {data: 'Сумма_платежа'},
            {data: 'ДатаПлатежа'}
        ],
        rowGroup: {
            dataSrc: 'Проект', // начальная группировка
            startRender: function (rows, group) {
                const collapsed = $('<span>▶</span>')
                    .css('cursor', 'pointer')
                    .on('click', function () {
                        rows.nodes().each(function (r) {
                            table.row(r).child.isShown() ?
                                table.row(r).child.hide() :
                                table.row(r).child(format(r.data())).show();
                        });
                        $(this).text(table.row(rows[0]).child.isShown() ? '▼' : '▶');
                    });
                return $('<tr/>')
                    .append($('<td/>').attr('colspan', 5).append(collapsed).append(' ' + group));
            }
        },
        pageLength: 50,
        responsive: true,
        language: {url: '/static/ru.json'}
    });
}


function format(data) {
    return '<div>Детали заказа: ' + data.Номер + '</div>';
}


function initAdditionalHandlers() {

    //document.addEventListener('DOMContentLoaded', function() {
    const mainTable = document.getElementById('mainTable');
    const selectedTable = document.getElementById('selectedTable');
    const filterInput = document.getElementById('filterInput');

    var table = $('#mainTable').DataTable({
        autoWidth: true,
        pageLength: 25,
        paging: true,
        ordering: true,
        info: true,
        orderMulti: true,
        searching: true,
        language: {
            url: '/static/ru.json'
        }

    });

    var stable = $('#selectedTable').DataTable({
        pageLength: 25,
        paging: true,
        ordering: true,
        info: true,
        orderMulti: true,
        language: {
            url: '/static/ru.json'
        },
        columnDefs: [
            {width: '30px', targets: 0}, // Первый столбец — 150 px
            {width: '200px', targets: 1}, // Второй столбец — 200 px
            {width: '200px', targets: 11}
        ]
    });


    // Флаг: активен ли фильтр
    var filterActive = false;

    // Обработчик клика по кнопке
    $('#toggleFilterBtn').on('click', function () {
        if (!filterActive) {
            // Включаем фильтр: показываем ТОЛЬКО строки, где 3‑й столбец ПУСТОЙ
            $.fn.dataTable.ext.search.push(
                function (settings, data, dataIndex) {
                    var columnIndex = 3; // Индекс 3‑го столбца (0, 1, 2)
                    var cellValue = data[columnIndex];

                    // Проверяем, что значение пустое (null, undefined, пустая строка или пробелы)
                    return cellValue.toString().trim() === 'active';
                }
            );

            // Обновляем таблицу
            table.draw();

            // Меняем текст кнопки
            $(this).text('Показать все строки');
            filterActive = true;
        } else {
            // Отключаем фильтр
            $.fn.dataTable.ext.search.pop(); // Удаляем последний фильтр
            table.draw(); // Перерисовываем


            // Меняем текст кнопки
            $(this).text('Скрыть все снятые проекты');
            filterActive = false;
        }
    });

    // Обработчик клика по строке
    mainTable.addEventListener('click', function (event) {

        if (event.target.closest('tr')) {
            // Убираем выделение с других строк
            //mainTable.querySelectorAll('tr').forEach(tr => tr.classList.remove('highlight'));

            // Получаем текущую строку
            const currentRow = event.target.closest('tr');
            const id = currentRow.dataset.id;
            const name = currentRow.cells[1].innerText;
            const description = currentRow.cells[2].innerText;
            const status = currentRow.cells[3].innerText;

            // Подгружаем данные во вторую таблицу
            sendRequest('/eis_ext/' + id, '#selectedTable')
        }
    });
}


// Инициализация модального окна
var myModal

function modalWnd() {
    myModal = new bootstrap.Modal('#myModal', {
        backdrop: 'static', // Запретить закрытие по клику вне окна
        keyboard: false // Запретить закрытие по клавише Esc
    });
    // События модального окна
    myModal.addEventListener('show.bs.modal', function () {
        // Действия при открытии
    });

    myModal.addEventListener('hidden.bs.modal', function () {
        // Действия при закрытии
    });
}


function update_adm(tableName) {
    // Создаем объект XMLHttpRequest
    const xhr = new XMLHttpRequest();

    // Настраиваем запрос (метод POST, URL сервера, асинхронный режим)
    xhr.open('POST', 'adminp/update_table', true);

    // Устанавливаем заголовок Content-Type для передачи данных в формате JSON
    xhr.setRequestHeader('Content-Type', 'application/json');

    // Отправляем запрос без ожидания ответа
    xhr.send(JSON.stringify({tableName: tableName}));

    // Можно добавить минимальную обратную связь для пользователя
    console.log(`Отправлен запрос на обновление таблицы: ${tableName}`);
}


var popup = document.getElementById('popup');
var closeBtn = document.querySelector('.close-btn');
var popupHeader = document.getElementById('popupHeader');

var isDown = false;
var initialX, initialY, initialMouseX, initialMouseY;

var shiftX, shiftY;

// Функция для открытия окна (вызывается из HTML по клику)
function openPopupWindow(id) {

    sendRequest("/eis/rez", "#popup-content", id)
    initPopUp();
    popup.classList.add('active');
}

function openPopupWindowSale(id, year) {

    sendRequest("/eis/sale/" + id, "#popup-content", year)
    initPopUp();
    popup.classList.add('active');
}


function initPopUp() {
    popup = document.getElementById('popup');
    closeBtn = document.querySelector('.close-btn');
    popupHeader = document.getElementById('popupHeader');


    // Закрытие окна по клику на кнопку «×»
    closeBtn.addEventListener('click', () => {
        popup.classList.remove('active');
    });

// Закрытие окна при клике вне его
    window.addEventListener('click', (e) => {
        if (e.target === popup) {
            popup.classList.remove('active');
        }
    });

// Логика перетаскивания окна
    popupHeader.addEventListener('mousedown', (e) => {
        isDown = true;
        initialX = popup.offsetLeft;
        initialY = popup.offsetTop;
        initialMouseX = e.clientX;
        initialMouseY = e.clientY;
    });

    document.addEventListener('mouseup', () => {
        isDown = false;
    });

    document.addEventListener('mousemove', (e) => {
        if (!isDown) return;
        e.preventDefault();
        shiftX = e.clientX - initialMouseX;
        shiftY = e.clientY - initialMouseY;
        popup.style.left = initialX + shiftX + 'px';
        popup.style.top = initialY + shiftY + 'px';
    });

}


function jampToAlterCode(id, val){
    let table = $(id).DataTable(); // Получаем экземпляр DataTable
     table.search(val).draw(); // Устанавливаем поиск и обновляем таблицу
}






