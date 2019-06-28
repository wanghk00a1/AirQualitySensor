function loadAQIFunction() {

    const aqiPredictTitle = 'AQI Predicting Value';
    const aqiRealTitle = 'AQI Real Value';
    const aqiPredictOption = generateGaugeChartOption(aqiPredictTitle, 'aqi-predict');
    const aqiRealOption = generateGaugeChartOption(aqiRealTitle, 'aqi-real');

    const aqiPredictChart = echarts.init(document.getElementById('aqi-predict'));
    const aqiRealChart = echarts.init(document.getElementById('aqi-real'));

    setInterval(() => {
        aqiPredictChart.setOption({
            ...aqiPredictOption,
            series: [{
                ...aqiPredictOption.series[0],
                data: [{name: '',value: generateRandomInt(500)}]
            }]
        });
        aqiRealChart.setOption({
            ...aqiRealOption,
            series: [{
                ...aqiRealOption.series[0],
                data: [{name: '',value: generateRandomInt(500)}]
            }]
        });
    }, 1000);
}

$(document).ready(loadAQIFunction);

/**
 *
 * @param title {string}
 * @param name {string}
 * @returns {{series: {data: {name: string, value: number}[], name: string, detail: {formatter: string}, type: string}[], tooltip: {formatter: string}, toolbox: {feature: {saveAsImage: {}, restore: {}}}}}
 */
function generateGaugeChartOption(title, name) {
    return {
        tooltip : {
            formatter: "{a} <br/>{b} : {c}%"
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
                detail: {formatter:'{value}'},
                data: [{value: 50, name: title}]
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