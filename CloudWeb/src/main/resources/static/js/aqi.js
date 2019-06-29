function loadAQIFunction() {

    const aqiPredictTitle = 'Predict';
    const aqiRealTitle = 'Real';
    const aqiPredictOption = generateGaugeChartOption(aqiPredictTitle, 'aqi-predict');
    const aqiRealOption = generateGaugeChartOption(aqiRealTitle, 'aqi-real');

    const aqiPredictChart = echarts.init(document.getElementById('aqi-predict'));
    const aqiRealChart = echarts.init(document.getElementById('aqi-real'));
    const setAqiAccuracyValue = (function() {
        const $aqiAccuracyText = document.getElementById('accuracy-value');
        const $aqiAccuracyChart = document.getElementById('accuracy-chart');
        /**
         * @param {number} accuracy
         */
        return function (accuracy) {
            $aqiAccuracyText.innerText = `${accuracy}%`;
            $aqiAccuracyChart.style.height = `${accuracy + 20}%`;
        };
    })();

    setInterval(async () => {
        const expressions = ['😐', '😢', '😊'];
        try {
            const { predict, real, accuracy } = await API.fetchAQIStatistic();
            aqiPredictChart.setOption({
                ...aqiPredictOption,
                series: [{
                    ...aqiPredictOption.series[0],
                    data: [{name: aqiPredictTitle,value: predict}]
                }]
            });
            aqiRealChart.setOption({
                ...aqiRealOption,
                series: [{
                    ...aqiRealOption.series[0],
                    data: [{name: aqiRealTitle,value: real}]
                }]
            });
            setAqiAccuracyValue(accuracy);
            window.$lanuchBarrageMulti(
                new Array(parseInt(Math.random() * 20, 10) )
                    .fill(0)
                    .map(() => Math.random()
                        .toString(36)
                        .slice(2)
                        .padStart(Math.random() * 20, Math.random().toString(36).slice(3, 4))
                        + expressions[parseInt(Math.random() * expressions.length, 10)]
                    ),
                {
                color: 'white',
                padding: '4px',
            });
        } catch(err) {
            alert(err.message);
        }
    }, 800 + Math.random() * 1000);
}

$(document).ready(loadAQIFunction);

const API = {
    AQI_STATISTIC: '/topic/consumeStatistic',
    fetchAQIStatistic: async () => {
        try {
            // const response = await fetch(this.AQI_STATISTIC);
            // const json = response.json();
            return {
                predict: generateRandomInt(500),
                real: generateRandomInt(500),
                accuracy: generateRandomInt(100),
            };
        } catch (err) {
            console.error(err);
            throw new Error('[Error] Unable to fetch aqi data');
        }
    }
};

/**
 *
 * @param title {string}
 * @param name {string}
 * @returns {{series: {data: {name: string, value: number}[], name: string, detail: {formatter: string}, type: string}[], tooltip: {formatter: string}, toolbox: {feature: {saveAsImage: {}, restore: {}}}}}
 */
function generateGaugeChartOption(title, name) {
    return {
        tooltip : {
            formatter: "{a} <br/>{b} : {c}"
        },
        series: [
            {
                name: name,
                type: 'gauge',
                min: 1,
                max: 500,
                splitNumber: 6,
                axisLine: {
                  lineStyle: {
                      color: [
                          [0.1, '#00E400'],
                          [0.2, '#FFFF00'],
                          [0.3, '#FF7E00'],
                          [0.4, '#FF0000'],
                          [0.6, '#8f3f97'],
                          [1, '#7E0023']
                      ]
                  }
                },
                splitLine: {
                    show: false,
                },
                axisLabel: {
                  show: false,
                },
                title: {
                    show: true,
                },
                detail : {
                    formatter:'{value}',
                    borderRadius: 3,
                    backgroundColor: '#444',
                    borderColor: '#aaa',
                    shadowBlur: 5,
                    shadowColor: '#333',
                    shadowOffsetX: 0,
                    shadowOffsetY: 3,
                    borderWidth: 0.5,
                    textBorderColor: '#000',
                    textBorderWidth: 1,
                    textShadowBlur: 1,
                    textShadowColor: '#fff',
                    textShadowOffsetX: 0,
                    textShadowOffsetY: 0,
                    fontFamily: 'Ping Fang SC',
                    fontSize: 22,
                    width: 60,
                    offsetCenter:  [0, '55%'],
                    color: '#eee',
                    rich: {}
                },
                // detail: {formatter:'{value}'},
                data: [{value: 50, name: '完成'}]
            }
        ]
    };
}

/**
 *
 * @param max {number}
 * @returns {number}
 */
function generateRandomInt(max) {
    return Math.floor(Math.random() * Math.floor(max));
}