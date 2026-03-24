function initCanvas() {
    const canvas = document.getElementById('chart');
const ctx = canvas.getContext('2d');

// Данные для диаграммы
const data = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
 // Пример обработки массива
    //alert(jsArray);
jsArray.forEach(item => {
  const monthIndex = item[0] * 1 - 1; // Преобразуем 1–12 в 0–11
  data[monthIndex] = (item[2].replace(' ', '')).replace(',', '.') ;
  //alert(monthIndex + ' --- ' + item[2] + ' --- ' + data[monthIndex]);
});

//alert(data[0] + " - " + data);

const labels = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'];
const maxValue = Math.ceil((Math.max(...data) / 10000000)) * 10000000;
//alert(maxValue);
const barWidth = 40;
const barGap = 10;
const yAxisHeight = 100;

// Рисуем фон
ctx.fillStyle = '#79c4f5';
ctx.fillRect(0, 0, canvas.width, canvas.height);

// Рисуем ось Y
ctx.fillStyle = '#fff';
ctx.font = '16px Arial';
for (let i = 0; i <= maxValue/1000000; i += 10) {
    const y = canvas.height - 50 - (i / (maxValue/1000000) * (canvas.height - 70));
    ctx.fillText(i, 10, y);
    ctx.beginPath();
    ctx.moveTo(50, y);
    ctx.lineTo(55, y);
    ctx.stroke();
}

// Рисуем столбцы
data.forEach((value, index) => {
    const x = 60 + (barWidth + barGap) * index;
    const height = (value / 1000000 / (maxValue/1000000)) * (canvas.height - 70);
    ctx.fillStyle = 'rgb(255,235,59)';
    ctx.fillRect(x, canvas.height - height - 50, barWidth, height);
});
// Рисуем подписи
data.forEach((value, index) => {
    const x = 60 + (barWidth + barGap) * index;
    const height = (value / 1000000 / (maxValue/1000000)) * (canvas.height - 70);
    ctx.fillStyle = 'rgba(243,242,238,0.55)';
    ctx.fillRect(x + barWidth / 2 - 2, canvas.height - height - 76, (formatNumber(value)).length * 8 + 4, 20);
    ctx.fillStyle = '#03249c';
    ctx.fillText( formatNumber(value), x + barWidth / 2, canvas.height - height - 60 );
});


// Рисуем ось X
ctx.beginPath();
ctx.moveTo(55, canvas.height - 50);
ctx.lineTo(canvas.width - 10, canvas.height - 50);
ctx.stroke();

// Рисуем подписи под столбцами
labels.forEach((label, index) => {
    const x = 60 + (barWidth + barGap) * index + barWidth / 2;
    ctx.fillText(label, x, canvas.height - 30);
});


}

function formatNumber(value, options = {}) {
  const {
    decimals = 2,
    locale = 'ru-RU',
    asCurrency = false,
    currency = 'RUB'
  } = options;

  const formatter = new Intl.NumberFormat(locale, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
    useGrouping: true
  });

  let formatted = formatter.format(value);

  if (asCurrency) {
    const currencyFormatter = new Intl.NumberFormat(locale, {
      style: 'currency',
      currency: currency,
      minimumFractionDigits: decimals
    });
    formatted = currencyFormatter.format(value);
  }

  return formatted;
}
