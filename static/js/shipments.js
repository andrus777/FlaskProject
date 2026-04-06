function initShipmentsCanvas() {
    // Данные для диаграммы 1
    const data2025 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    // Обработка переданного json массива вида {"2025","1","262","369554.52","686635","779 737 086,95"} {Год, Месяц, Единицы, Сумма, Единица_за_год, Сумма_за_год}
    jsArraySh.forEach(item => {
        if (item[0] == '2025') {
            const monthIndex = item[1] * 1 - 1; // Преобразуем 1–12 в 0–11
            data_values[monthIndex] = item[3];
        }
    });
    initSh('chart_2025', data2025);

    const data2026 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    jsArraySh.forEach(item => {
        if (item[0] == '2026') {
            const monthIndex = item[1] * 1 - 1; // Преобразуем 1–12 в 0–11
            data2026[monthIndex] = item[3];
        }
    });
    initSh('chart_2026', data2026);
}

function initSh(chartName, data) {
    const canvas = document.getElementById(chartName);
    const labels = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']; // Массив значений для оси времени (номера месяцев)
    const maxValue = Math.ceil((Math.max(...data) / 10000000)) * 10000000; // Определяем максимальное значение для оси Y
    const barWidth = 40;
    const barGap = 10;
    // Массив для хранения координат прямоугольников
    const rectangles = [];

    rectangles.length = 0; // Очищаем массив перед перерисовкой

    data.forEach((value, index) => {
        const x = 50 + index * (barWidth + spacing);
        const height = value * scale;
        const y = canvas.height - 50 - height; // Отступ снизу 50px

        // Сохраняем координаты в массив
        rectangles.push({
            x,
            y,
            width: barWidth,
            height,
            value
        });
    });

        canvas.addEventListener('click', (e) => {
            if (highlightedIndex === -1) return;

            const clickedRect = rectangles[highlightedIndex];
            console.log('Клик по столбцу с значением:', clickedRect.id);
            ShowModal(clickedRect.id); // Вызываем функцию с данными столбца
        });


        canvas.addEventListener('mousemove', (e) => {
            const rect = canvas.getBoundingClientRect();
            const mouseX = e.clientX - rect.left;
            const mouseY = e.clientY - rect.top;

            let hoveredIndex = -1;

            // Проверяем, находится ли курсор над каким‑либо прямоугольником
            for (let i = 0; i < rectangles.length; i++) {
                const rect = rectangles[i];
                if (
                    mouseX >= rect.x &&
                    mouseX <= rect.x + rect.width &&
                    mouseY >= rect.y &&
                    mouseY <= rect.y + rect.height
                ) {
                    hoveredIndex = i;
                    break;
                }
            }
            // Если индекс изменился — перерисовываем с подсветкой
            if (hoveredIndex !== highlightedIndex) {
                highlightedIndex = hoveredIndex;
                drawChartWithHighlight();
            }
        });

        drawChart(canvas);

}


function drawChart(cnv){
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, cnv.width, cnv.height);

    // Рисуем фон
    ctx.fillStyle = '#79c4f5';
    cnv.fillRect(0, 0, cnv.width, cnv.height);

    // Рисуем ось Y
    ctx.font = '16px Arial';
    for (let i = 0; i <= maxValue / 1000000; i += 10) {
        const y = canvas.height - 50 - (i / (maxValue / 1000000) * (canvas.height - 70));
        ctx.fillText(i, 10, y);
        ctx.beginPath();
        ctx.moveTo(50, y);
        ctx.lineTo(55, y);
        ctx.stroke();

        // Подписи
        ctx.fillStyle = '#000';
        ctx.font = '12px Arial';
        ctx.textAlign = 'center';
        ctx.fillText(formatNumber(rect.value), rect.x + rect.width / 2, rect.y);
    }

     // Рисуем ось X
    ctx.beginPath();
    ctx.moveTo(55, canvas.height - 50);
    ctx.lineTo(canvas.width - 10, canvas.height - 50);
    ctx.stroke();

    // Рисуем подписи
    data.forEach((value, index) => {
        const x = 60 + (barWidth + barGap) * index;
        const height = (value / 1000000 / (maxValue / 1000000)) * (canvas.height - 70);
        ctx.fillStyle = 'rgba(243,242,238,0.55)';
        ctx.fillRect(x + barWidth / 2 - 2, canvas.height - height - 76, (formatNumber(value)).length * 8 + 4, 20);
        ctx.fillStyle = '#03249c';
        ctx.fillText(formatNumber(value), x + barWidth / 2, canvas.height - height - 60);
    });

}


function drawChartWithHighlight() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        rectangles.forEach((rect, index) => {
            // Устанавливаем цвет в зависимости от подсветки
            ctx.fillStyle = index === highlightedIndex ? '#5dff22' : '#F6D602FF';
            ctx.fillRect(rect.x, rect.y, rect.width, rect.height);

            // Подписи
            ctx.fillStyle = '#000';
            ctx.font = '12px Arial';
            ctx.textAlign = 'center';
            ctx.fillText(formatNumber(rect.value), rect.x + rect.width / 2, rect.y);
        });
    }


// Заглушка для функции ShowModal
    function ShowModal(id) {
        alert(`Открываем модальное окно для значения: ${id}`);
    }



