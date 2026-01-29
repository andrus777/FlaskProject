// Функция для отправки запроса
function sendRequest(url, sect) {

    $(sect).html('<img src="/static/loading.gif" alt="Изображение">');
//alert(sect+ "  " + $(sect).html());
    $.ajax({
        type: 'POST',
        url: url,
        success: function (response) {
            // Вставляем полученный HTML в контейнер
            //alert(response);
            $(sect).html(response);
            //alert(url);
            if (url == "/eis/store") {
                //alert("store");
                initStoreHandlers();
            }
            if (url == "/eis/projects") {
                //alert("eispro");
                initAdditionalHandlers();
            }

            if (url=="/eis/project/"){
                $('#myModal').modal('show');
            }
        },
        error: function (error) {
            console.error('Ошибка:', error);
            $(sect).html('Произошла ошибка');
        }
    });
}


function initStoreHandlers() {
    const exTable = document.getElementById('exTable');
    //alert(exTable);
    $('#exTable').DataTable({
        autoWidth: true,
        scrollY: '500px',
        scrollX: true,
        scrollCollapse: true,
        paging: true,
        ordering: true,
        orderMulti: true,
        info: true,
        searching: true,
        language: {
            url: '/static/ru.json'}
    });

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
            url: '/static/ru.json'}
    });



  // Флаг: активен ли фильтр
  var filterActive = false;

  // Обработчик клика по кнопке
  $('#toggleFilterBtn').on('click', function() {
    if (!filterActive) {
      // Включаем фильтр: показываем ТОЛЬКО строки, где 3‑й столбец ПУСТОЙ
      $.fn.dataTable.ext.search.push(
        function(settings, data, dataIndex) {
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

            // Добавляем выделение
            //currentRow.classList.add('highlight');

            // Подгружаем данные во вторую таблицу
            sendRequest('/eis_ext/' + id, '#selectedTable')
        }
    });

    // Функция фильтрации
    // const filterTable = () => {
    //     const filterValue = filterInput.value.toLowerCase();
    // Обработка клавиш
    //     if(filterValue.length > 3) {
    //        sendRequest('/store_count/' + filterValue, '#mainTable')
    //     }
    //  };

    // Обработчик ввода в поле фильтра
    // filterInput.addEventListener('input', filterTable);


}


// Инициализация модального окна
const myModal = new bootstrap.Modal('#myModal', {
 backdrop: 'static', // Запретить закрытие по клику вне окна
 keyboard: false // Запретить закрытие по клавише Esc
});

// События модального окна
myModal.addEventListener('show.bs.modal', function() {
 // Действия при открытии
});

myModal.addEventListener('hidden.bs.modal', function() {
 // Действия при закрытии
});






