const AQI_LEVEL = {
    'Good': 0,
    'Moderate': 1,
    'Unhealthy for Sensitive Groups': 2,
    'Unhealthy': 3,
    'Very Unhealthy': 4,
    'Hazardous': 5,
};
const AQI_LEVEL_EN = [
    'Good',
    'Moderate',
    'Unhealthy for Sensitive Groups',
    'Unhealthy',
    'Very Unhealthy',
    'Hazardous',
];
const AQI_LEVEL_ZH = [
    '好',
    '中等',
    '不适于敏感人群',
    '不健康',
    '非常不健康',
    '危险',
];

const AQI_COLOR = [
    '#00E400',
    '#FFFF00',
    '#FF7E00',
    '#FF0000',
    '#8f3f97',
    '#7E0023'
];

/**
 * 计算 AQI 值范围
 * @param {number} value
 * @returns {number}
 */
function calculateAQILevel(value) {
    if (value <= 50) return AQI_LEVEL.Good;
    if (value <= 100) return AQI_LEVEL.Moderate ;
    if (value <= 150) return AQI_LEVEL["Unhealthy for Sensitive Groups"];
    if (value <= 200) return AQI_LEVEL.Unhealthy;
    if (value <= 300) return AQI_LEVEL["Very Unhealthy"];
    if (value <= 500) return AQI_LEVEL.Hazardous;
    throw new Error('Error Value');
}

function loadAQIFunction() {
    let actualAQIPoints = []; // 缓存真实 AQI 数据
    let predictAQIPoints = []; // 缓存预测 AQI 数据
    // AQI 预测值展示
    const setAqiPredictValue = (function () {
        const $aqiPredictBG = $('#aqi-predict');
        const $aqiPredictENText = $('#aqi-predict .en');
        const $aqiPredictZHText = $('#aqi-predict .zh');
        const $aqiPredictValueText = $('#aqi-predict .value');
        /**
         * @param {number} value
         */
        return function (value) {
            const level = calculateAQILevel(value);
            $aqiPredictBG.css('background-color', AQI_COLOR[level]);
            $aqiPredictENText.text(AQI_LEVEL_EN[level]);
            $aqiPredictZHText.text(AQI_LEVEL_ZH[level]);
            $aqiPredictValueText.text(`AQI: ${value}`);
        };
    })();
    // AQI 真实值展示
    const setAqiActualValue = (function () {
        const $aqiActualBG = $('#aqi-actual');
        const $aqiActualENText = $('#aqi-actual .en');
        const $aqiActualZHText = $('#aqi-actual .zh');
        const $aqiActualValueText = $('#aqi-actual .value');
        /**
         * @param {number} value
         */
        return function (value) {
            const level = calculateAQILevel(value);
            $aqiActualBG.css('background-color', AQI_COLOR[level]);
            $aqiActualENText.text(AQI_LEVEL_EN[level]);
            $aqiActualZHText.text(AQI_LEVEL_ZH[level]);
            $aqiActualValueText.text(`AQI: ${value}`);
        };
    })();
    // 准确率展示
    const setAqiAccuracyValue = (function() {
        const $aqiAccuracyText = document.getElementById('accuracy-value');
        const $aqiAccuracyChart = document.getElementById('accuracy-chart');
        /**
         * @param {number} accuracy
         */
        return function (accuracy) {
            $aqiAccuracyText.innerText = `${accuracy}%`;
            $aqiAccuracyChart.style.height = `${accuracy + 18}%`;
        };
    })();
    // AQI 线图
    const setAqiLineChartData = (function () {
        const aqiLineChartOption = generateAQILineChartOption();
        const $aqiLineChart = echarts.init(document.getElementById('aqi-line-chart'));
        const $aqiTable = document.getElementById('aqi-table');
        return function ({comingPredictAQIPoints = [], comingActualAQIPoints = []}) {
            predictAQIPoints = mergeAndSortPoints(predictAQIPoints, comingPredictAQIPoints);
            actualAQIPoints = mergeAndSortPoints(actualAQIPoints, comingActualAQIPoints);
            setAqiAccuracyValue(calculateAccuracy(actualAQIPoints, predictAQIPoints));
            $aqiLineChart.setOption({
                ...aqiLineChartOption,
                xAxis: { type: 'time' },
                yAxis: {
                    // type: 'category',
                    min: 1,
                    max: 200,
                },
                series: [{
                    ...aqiLineChartOption.series[0],
                    data: predictAQIPoints.map(({timestamp, random_tree}) => [new Date(timestamp), random_tree /* AQI_LEVEL_EN[calculateAQILevel(random_tree)] */]),
                }, {
                    ...aqiLineChartOption.series[1],
                    data: actualAQIPoints.map(({timestamp, aqi}) => [new Date(timestamp), aqi /* AQI_LEVEL_EN[calculateAQILevel(aqi)] */]),
                }],
            });
            const sameIntervalActualAQIPoints = produceSameIntervalPoints(actualAQIPoints, predictAQIPoints);
            const html = `
            <table class="table">
                <thead>
                    <tr>
                        <th class="head">Time</th>
                        ${predictAQIPoints.map(({timestamp}) => `<th>${formatTimeString(timestamp)}</th>`)}
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td class="head">Predict AQI Value</td>
                        ${predictAQIPoints.map(({random_tree}) => {
                            const level = calculateAQILevel(random_tree);
                            return `<td><span style="border: solid 1px ${AQI_COLOR[level]}">${random_tree}</span></td>`;
                        })}
                    </tr>
                    <tr>
                        <td class="head">Predict AQI Level</td>
                        ${predictAQIPoints.map(({random_tree}) => {
                            const level = calculateAQILevel(random_tree);
                            return `<td><span style="color: ${AQI_COLOR[level]}">${AQI_LEVEL_EN[level]}</span></td>`;
                        })}
                    </tr>
                    <tr>
                        <td class="head">Actual AQI Level</td>
                        ${sameIntervalActualAQIPoints.map(({timestamp}) => {
                            return `<td>${formatTimeString(timestamp)}</td>`;
                        })}
                    </tr>
                    <tr>
                        <td class="head">Actual AQI Level</td>
                        ${sameIntervalActualAQIPoints.map(({aqi}) => {
                            const level = calculateAQILevel(aqi);
                            return `<td><span style="color: ${AQI_COLOR[level]}">${AQI_LEVEL_EN[level]}</span></td>`;
                        })}
                    </tr>
                </tbody>
            </table>
            `;
            $aqiTable.innerHTML = html.trim().replace(/,/g, '');
        };
    })();
    const setTweetLineChartData = (function () {
        const tweetLineChartOption = generateCountLineChartOption();
        const $tweetLineChart = echarts.init(document.getElementById('tweets-line-chart'));
        return function (values) {
            predictAQIPoints = mergeAndSortPoints(predictAQIPoints, values);
            $tweetLineChart.setOption({
                ...tweetLineChartOption,
                xAxis: {
                    type: 'time',
                },
                series: [
                    {
                        ...tweetLineChartOption.series[0],
                        data: predictAQIPoints.map(({timestamp, positive}) => ([new Date(timestamp), positive]))
                    },
                    {
                        ...tweetLineChartOption.series[1],
                        data: predictAQIPoints.map(({timestamp, negative}) => ([new Date(timestamp), negative]))
                    },
                    {
                        ...tweetLineChartOption.series[2],
                        data: predictAQIPoints.map(({timestamp, total}) => ([new Date(timestamp), total]))
                    },
                    {
                        ...tweetLineChartOption.series[3],
                        data: predictAQIPoints.map(({timestamp, w_positive}) => ([new Date(timestamp), w_positive]))
                    },
                    {
                        ...tweetLineChartOption.series[4],
                        data: predictAQIPoints.map(({timestamp, w_negative}) => ([new Date(timestamp), w_negative]))
                    },
                    {
                        ...tweetLineChartOption.series[5],
                        data: predictAQIPoints.map(({timestamp, w_total}) => ([new Date(timestamp), w_total]))
                    },
                ]
            });
        };
    })();

//    const expressions = ['😐', '😢', '😊'];
    // 定时获取数据
    async function fetchData(city = 'LONDON', limit = 5) {
        try {
            const actualAQIs = await API.fetchActualAQI(city, limit);
            const predictAQIs = await API.fetchPredictAQI(city, limit * 12);
            setAqiPredictValue(predictAQIs[0]['random_tree']);
            setAqiActualValue(actualAQIs[0]['aqi']);
            setAqiAccuracyValue(generateRandomInt(100));
            setAqiLineChartData({
                comingPredictAQIPoints: predictAQIs.reverse(),
                comingActualAQIPoints: actualAQIs.reverse(),
            });
            setTweetLineChartData(predictAQIs.reverse());
        } catch(err) {
            alert(err.message);
        }
    }
    let cityName = getParameterByName('city')
    if (!cityName) cityName = 'LONDON'; 
    fetchData(cityName, 30).then(() => {
        setInterval(() => fetchData(cityName, 30), 60000);
    });
    console.log('[LONDON, NY] is suppoerted.');
}

$(document).ready(loadAQIFunction);

const API = {
    ACTUAL_AQI_BY_CITY: '/api/getActualAqiByCity',
    PREDICT_AQI_BY_CITY: '/api/getPredictAqiByCity',
    /**
     * 获取实际 AQI
     * @param {string} city
     * @param {number} limit
     * @returns {Promise<Array<{city: string, timestamp: string, aqi: number}>>}
     */
    async fetchActualAQI(city = 'LONDON', limit = 10) {
        try {
            const response = await fetch(`${this.ACTUAL_AQI_BY_CITY}?city=${city}&limit=${limit}`);
            const json = await response.json();
            return json;
        } catch (err) {
            console.error(err);
            throw new Error('[Error] Unable to fetch aqi data');
        }
    },
    /**
     * 获取预测 AQI
     * @param {string} city
     * @param {number} limit
     * @returns {Promise<Array<{city: string, timestamp: string, positive: number}>>}>}
     */
    async fetchPredictAQI(city = 'LONDON', limit = 10) {
        try {
            const response = await fetch(`${this.PREDICT_AQI_BY_CITY}?city=${city}&limit=${limit}`);
            const json = await response.json();
            return json;
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

function generateCountLineChartOption() {
    return {
        tooltip: {
            trigger: 'axis'
        },
        legend: {
            data: ['positive', 'negative', 'total', 'w_positive', 'w_negative', 'w_total']
        },
        xAxis: {
            type: 'time',
        },
        yAxis: {
            type: 'value'
        },
        series: [
            {
                name: 'positive',
                type: 'line',
            },
            {
                name: 'negative',
                type: 'line',
            },
            {
                name: 'total',
                type: 'line',
            },
            {
                name: 'w_positive',
                type: 'line',
            },
            {
                name: 'w_negative',
                type: 'line',
            },
            {
                name: 'w_total',
                type: 'line',
            },
        ],
    };
}

function generateAQILineChartOption () {
    return {
        tooltip: {
            trigger: 'axis'
        },
        legend: {
            data: ['Predicting AQI', 'Actual AQI']
        },
        xAxis: {
            type: 'time',
        },
        yAxis: {
            type: 'value'
        },
        series: [
            {
                name: 'Predicting AQI',
                type: 'line',
            },
            {
                name: 'Actual AQI',
                type: 'line',
            },
        ],
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

/**
 * 合并去重时间点 并取后 80 个点
 * @param {Array<object>} currentPoints
 * @param {Array<object>} comingPoints
 * @returns {Array<object>}
 */
function mergeAndSortPoints(currentPoints, comingPoints) {
    let points = currentPoints.concat(comingPoints);
    points.sort(({timestamp: t1}, {timestamp: t2}) => {
        return new Date(t1).getTime() - new Date(t2).getTime()
    });
    let result = [];
    for (let idx = 0; idx < points.length - 1; ++idx) {
        if (points[idx].timestamp !== points[idx + 1].timestamp) {
            result.push(points[idx]);
        }
    }
    result.push(points[points.length - 1]);
    if (result.length > 80) {
        // result = result.slice(-80);
    }
    return result;
}

/**
 * @param {Array<{timestamp: string, aqi: number}>} actualPoints
 * @param {Array<{timestamp: string, random_tree: number}>} predictPoints
 */
function calculateAccuracy(actualPoints, predictPoints) {
    actualPoints = [...actualPoints.slice(-12)].reverse();
    predictPoints = [...predictPoints].reverse();
    let matchedCount = 0;
    let totalCount = 0;
    for (let {timestamp: actualTS, aqi} of actualPoints) {
        const at = new Date(actualTS);
        for (let {timestamp: predictTS, random_tree} of predictPoints) {
            const pt = new Date(predictTS);
            if ([
                at.getFullYear() === pt.getFullYear(),
                at.getMonth() === pt.getMonth(),
                at.getDate() === pt.getDate(),
                at.getHours() === pt.getHours()
            ].every(v => v)) {
                ++totalCount;
                if (calculateAQILevel(random_tree) === calculateAQILevel(aqi)) {
                    ++matchedCount;
                }
            }
        }
    }
    if (totalCount === 0) return 0;
    return (matchedCount / totalCount * 100);
}

/**
 * @param {Array<{timestamp: string, aqi: number}>} actualPoints
 * @param {Array<{timestamp: string, random_tree: number}>} predictPoints
 */
function produceSameIntervalPoints(actualPoints, predictPoints) {
    const result = [];
    for (let {timestamp: predictTS} of predictPoints) {
        const pt = new Date(predictTS);
        for (let {timestamp: actualTS, aqi} of actualPoints) {
            const at = new Date(actualTS);
            if ([
                at.getFullYear() === pt.getFullYear(),
                at.getMonth() === pt.getMonth(),
                at.getDate() === pt.getDate(),
                at.getHours() === pt.getHours()
            ].every(v => v)) {
                result.push({timestamp: actualTS, aqi});
            }
        }
    }
    return result;
}

function formatTimeString(timestamp) {
    const d = new Date(timestamp);
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\\[").replace(/[\]]/, "\\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
    results = regex.exec(location.search);
    return results == null ? "": decodeURIComponent(results[1]);
}
