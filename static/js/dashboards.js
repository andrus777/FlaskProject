class Dashboard {
    constructor(options) {
        // Основные свойства
        this.canvas = options.canvas;
        this.ctx = this.canvas.getContext('2d');
        this.width = options.width || this.canvas.width;
        this.height = options.height || this.canvas.height;
        this.canvasColor = options.canvasColor || '#f5f5f5';
        this.barColor = options.barColor || '#4285f4';
        this.hoverColor = options.hoverColor || '#3367d6';
        this.data = options.data || {};
        this.padding = options.padding || 80; // Отступы от краёв
        this.barWidth = options.barWidth || 40; // Ширина столбцов
        this.gap = options.gap || 20; // Расстояние между столбцами
        this.onBarClick = options.onBarClick || null; // Callback для клика


        // Внутренние свойства для отслеживания состояния
        this.hoveredBar = null;
        this.bars = []; // Массив с координатами и данными столбцов

        // Инициализация
        this.init();
    }

    init() {
        this.calculateAxes();
        this.prepareBars();
        this.setupEventListeners();
        this.hoveredBarIndex = -1;
        this.previousHoveredIndex = -1;
    }

    // Расчёт осей и масштаба
    calculateAxes() {
        const values = Object.values(this.data);
        //this.maxValue = Math.max(...values) || 1;
        this.maxValue =  Math.ceil(Math.max(...values) / 10000000) * 10000000 || 1;
        this.yScale = (this.height - 2 * this.padding) / this.maxValue;
        this.xLabels = Object.keys(this.data);
    }


    // Подготовка данных для отрисовки столбцов
    prepareBars() {
        this.bars = this.xLabels.map((label, index) => {
            const x = this.padding + index * (this.barWidth + this.gap);
            const value = this.data[label];
            const height = value * this.yScale;
            const y = this.height - this.padding - height;

            return {
                x,
                y,
                width: this.barWidth,
                height,
                value,
                label,
                isHovered: false
            };
        });
        console.log('Координаты столбца:', this.bars);
    }

    // Настройка обработчиков событий
    setupEventListeners() {
        this.canvas.addEventListener('mousemove', (e) => this.handleMouseMove(e));
        this.canvas.addEventListener('click', (e) => this.handleClick(e));
        this.canvas.addEventListener('mouseleave', () => this.handleMouseLeave());
    }

    // Обработка наведения курсора
    handleMouseMove(event) {
        const rect = this.canvas.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;


        let hoveredIndex = -1;
        //const buffer = 5; // буфер для удобства наведения

        // Ищем столбец под курсором
        for (let i = 0; i < this.bars.length; i++) {
            const bar = this.bars[i];
            if (
                x >= bar.x  &&
                x <= bar.x + bar.width  &&
                y >= bar.y  &&
                y <= bar.y + bar.height
            ) {
                hoveredIndex = i;
                break;
            }
        }



        // Обновляем флаги isHovered для всех столбцов
        this.bars.forEach((bar, index) => {
            bar.isHovered = (index === hoveredIndex);
        });

        // Сохраняем индекс текущего наведённого столбца
        this.hoveredBarIndex = hoveredIndex;

        // Перерисовываем только если состояние изменилось
        if (hoveredIndex !== this.previousHoveredIndex) {
            this.previousHoveredIndex = hoveredIndex;
            this.draw();
        }
    }


    handleMouseLeave() {
        // Сбрасываем состояние наведения
        this.bars.forEach(bar => {
            bar.isHovered = false;
        });
        this.hoveredBarIndex = -1;
        this.previousHoveredIndex = -1;
        this.draw(); // Перерисовываем без наведения
    }

    // Обработка клика
    handleClick(event) {
        const rect = this.canvas.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;

        for (const bar of this.bars) {
            if (
                x >= bar.x &&
                x <= bar.x + bar.width &&
                y >= bar.y &&
                y <= bar.y + bar.height &&
                this.onBarClick
            ) {
                this.onBarClick(bar.label, bar.value);
                break;
            }
        }
    }

    // Отрисовка всего дашборда
    draw() {
        // Очистка канвы
        this.ctx.clearRect(0, 0, this.width, this.height);

        // Заливка фона канвы
        this.ctx.fillStyle = this.canvasColor;
        this.ctx.fillRect(0, 0, this.width, this.height);

        // Отрисовка осей
        this.drawAxes();

        // Отрисовка столбцов
        this.bars.forEach(bar => {
            const color = bar.isHovered ? this.hoverColor : this.barColor;
            this.ctx.fillStyle = color;
            this.ctx.fillRect(bar.x, bar.y, bar.width, bar.height);
        });

        // Отрисовка подписей
        this.drawLabels();
    }

    // Отрисовка осей координат
    drawAxes() {
        this.ctx.strokeStyle = '#333';
        this.ctx.lineWidth = 2;

        // Ось X
        this.ctx.beginPath();
        this.ctx.moveTo(this.padding, this.height - this.padding);
        this.ctx.lineTo(this.width - this.padding, this.height - this.padding);
        this.ctx.stroke();

        // Ось Y
        this.ctx.beginPath();
        this.ctx.moveTo(this.padding, this.padding);
        this.ctx.lineTo(this.padding, this.height - this.padding);
        this.ctx.stroke();
    }

    // Отрисовка подписей к осям и значений
    drawLabels() {
        this.ctx.fillStyle = '#333';
        this.ctx.font = '12px Arial';

        // Подписи по оси X (месяцы)
        this.bars.forEach((bar, index) => {
            const labelX = bar.x + bar.width / 2;
            const labelY = this.height - this.padding + 20 ;
            this.ctx.textAlign = 'left';
            this.ctx.fillText(bar.label, labelX, labelY);
        });

        // Подписи по оси Y (значения)
        const steps = 5;
        for (let i = 0; i <= steps; i++) {
            const value = (this.maxValue / steps) * i;
            const yPos = this.height - this.padding - (value * this.yScale);

            this.ctx.textAlign = 'right';
            this.ctx.fillText(Math.round(value/1000000) + ' млн', this.padding - 10, yPos + 4);

            // Горизонтальные линии сетки
            this.ctx.strokeStyle = '#ddd';
            this.ctx.beginPath();
            this.ctx.moveTo(this.padding, yPos);
            this.ctx.lineTo(this.width - this.padding, yPos);
            this.ctx.stroke();
        }
        // Рисуем подписи на графике над столбцами (значения)
        this.bars.forEach((bar, index) => {
             const lx = bar.x + 2;
             const ly = bar.y - 22;
             this.ctx.fillStyle = 'rgba(159,212,243,0.57)';
             this.ctx.fillRect(lx , ly, (formatNumber(bar.volume)).length * 8 + 10, 20);
             this.ctx.textAlign = 'left';
             this.ctx.fillStyle = '#03249c';
             this.ctx.fillText(formatNumber(bar.value), lx + 2  , ly + 17);
            });

    }
}
