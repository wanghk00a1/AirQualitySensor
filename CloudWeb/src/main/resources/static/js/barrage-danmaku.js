// jquery
$(document).ready(function () {
    console.log("弹幕页面开始加载");

    // init 弹幕插件
    let danmaku = new Danmaku();
    danmaku.init({
        container: document.getElementById('barrage-canvas'),
        comments: [],
        engine: 'DOM',
        speed: 144
    });

    // 默认显示的情绪为 nlp 的结果；否则显示naive bayes 的结果
    let sentiment = "nlp";
    let analysisMethod = document.getElementById("method");

    // 设置缓冲区，解决kafka 一次性读到大量数据的情况, 保存 nb & nlp 情感分析结果
    let barrageData = [];
    // detailed 缓冲区数据（来源是socket）
    let detailBarrageData = [];
    // detailed 显示过的数据（为了点击事件能索引到，会比socket 获取并存到缓冲区的数据速率 慢点）
    let detailDisplayData = []


    // stomp socket 客户端
    let stompClient = null;

    startSocket()

    // 开启socket
    function startSocket() {
        // 创建 socket 连接
        let socket = new SockJS('/endpointSang');
        stompClient = Stomp.over(socket);

        stompClient.heartbeat.outgoing = 15000;
        stompClient.heartbeat.incoming = 0;

        stompClient.connect({}, function (frame) {
            console.log('Connected:' + frame);
            // 启动时往socket /initSentiment 发条消息，触发kafka 线程
            stompClient.send("/initSentiment", {}, "hello world");

            // 订阅 /topic/initSentiment
            stompClient.subscribe('/topic/initSentiment', function (response) {
                lanuchBarrageOnce("😊" + response.body);
            })

            // 订阅 /topic/consumeTweets ，london & new york 的数据
            stompClient.subscribe('/topic/consumeTweets', function (response) {
                if (response.body == "ping-alive") {
                    console.log("consumeTweets alive")
                } else {
                    let status = JSON.parse(response.body);
                    //解析消息并加入弹幕缓冲区
                    barrageData.push(status)
                    if (barrageData.length > 2000) {
                        // 缓冲区弹幕过多，直接清理
                        barrageData.splice(50, 200);
                    }

                    // detailed barrage 数据保存并展示
                    detailBarrageData.push(status);
                }
            })
        });
    }

    window.startSocket = startSocket

    // 关闭socket
    function stopSocket() {
        if (stompClient != null) {
            // 通知后端停止线程订阅kafka消息
            stompClient.send("/updateConsumer", {}, "close");
            stompClient.disconnect();
            stompClient = null;
        }
        console.log('Disconnected socket');
    }

    window.stopSocket = stopSocket

    // 发送弹幕
    function lanuchBarrageOnce(message, styles = {}) {
        let comment = {
            text: message,
            // 默认为 rtl（从右到左），支持 ltr、rtl、top、bottom。
            mode: 'rtl',
            // 在使用 DOM 引擎时，Danmaku 会为每一条弹幕创建一个 <div> 节点，
            style: {
                fontSize: '20px',
                color: '#ffffff',
                // border: '1px solid #337ab7',
                // textShadow: '-1px -1px #000, -1px 1px #000, 1px -1px #000, 1px 1px #000',
                cursor: 'pointer',
            },
        };
        danmaku.emit(comment);
    }


    let intervalID;
    let basicSpeed = 100;

    // 定时器 显示缓冲区里的弹幕，优化弹幕显示效果
    function startTimer() {
        // 显示 flink core nlp 结果
        let message = barrageData.shift()
        if (message != undefined) {
            let emoji = message.sentiment == 1 ? "😍" : (message.sentiment == 0 ? "😐" : "😭");
            let line = emoji + " " + (message.text.length < 50 ? message.text : message.text.substr(0, 50) + "..");
            lanuchBarrageOnce(line)
        }
        intervalID = setTimeout(startTimer, basicSpeed + getRandomInt(100));
    }

    // 更改基础速率
    window.updateBasicBarrageTimer = function () {
        let inputText = document.querySelector('.interval-input');
        basicSpeed = inputEle.value;
        inputText.value = '';
        console.log("change barrage speed : " + basicSpeed)
    };

    // 启动弹幕显示
    // startTimer()

    //刷新or关闭浏览器前，先断开socket连接，onbeforeunload 在 onunload之前执行
    window.onbeforeunload = function () {
        if (stompClient != null) {
            // 通知后端停止线程订阅kafka消息
            stompClient.send("/updateConsumer", {}, "close");

            stompClient.disconnect();
            stompClient = null;
            console.log("stompClient disconnect");
        }
        console.log("onbeforeunload");
    }

    // 测试普通发射弹幕
    window.lanuchBarrage = function () {
        let inputEle = document.querySelector('.barrage-input');
        lanuchBarrageOnce(inputEle.value);
        inputEle.value = '';
    };

    // 弹幕基础操作
    window.basicOperation = function (opera) {
        switch (opera) {
            case 'show':
                danmaku.show()
                startTimer();
                break
            case 'hide':
                danmaku.hide()
                clearInterval(intervalID);
                break
            case 'clear':
                danmaku.clear()
                break
            case 'reset':
                stopSocket()
                startSocket()
                danmaku.clear()
                danmaku.show()
                clearInterval(intervalID)
                startTimer()
                break
            default:
                console.log("opera : " + opera)
        }
    };

    // switch 情感结果的分析方法
    window.switchAnalysisMethod = function (method) {
        switch (method) {
            case 'nlp':
                sentiment = "nlp";
                analysisMethod.innerHTML = "stanford core nlp";
                break
            case 'nb':
                sentiment = "nb";
                analysisMethod.innerHTML = "spark mllib naive bayes";
                break
            case 'dl':
                sentiment = "dl";
                analysisMethod.innerHTML = "deep learning";
                break
        }
    };


    let scrollUpIntervalId;
    let running; // 用于pause后暂停鼠标悬停事件
    let scrollUpBox = document.getElementById('scrollUpBox');
    // detail 弹幕部分悬停事件
    scrollUpBox.onmouseover = function () {
        clearInterval(scrollUpIntervalId);
    }
    scrollUpBox.onmouseout = function () {
        if (running)
            scrollUp(detailBarrageBasicSpeed);
    }

    // 自动滚屏
    function scrollUp(duration) {
        scrollUpIntervalId = setInterval(function () {
            if (scrollUpBox.scrollTop >= (content.clientHeight - scrollUpBox.clientHeight)) {
                scrollUpBox.scrollTop = 0;
            } else {
                scrollUpBox.scrollTop += 25;
            }
        }, duration)
    }


    let detailIntervalId;
    let detailBarrageBasicSpeed = 300;

    // 更改 detail barrage 基础速率
    window.updateDetailBarrageSpeed = function () {
        let inputText = document.querySelector('.detail-input');
        detailBarrageBasicSpeed = inputEle.value;
        inputText.value = '';
        console.log("change detail barrage speed : " + detailBarrageBasicSpeed)
    };

    // detailed 弹幕显示
    function displayDetailBarrage(duration) {
        running = true
        // clearInterval(detailIntervalId)
        detailIntervalId = setInterval(function () {
            // 不应是shift, 实际可以用游标记录显示过的数据位置, 这边新增个队列保存 display 的内容,效果也行
            let message = detailBarrageData.shift()
            if (message != undefined) {
                // message = {
                //     id: getRandomInt(10),
                //     text: "test" + getRandomInt(10),
                //     author: "Tommy Wang" + getRandomInt(10),
                //     nlpPolarity: "😐",
                //     nbPolarity: "😢",
                //     dlPolarity: "😊",
                //     date: "Sun Apr  7 16:27:05 HKT 2019",
                //     latitude: getRandomInt(100),
                //     longitude: getRandomInt(100),
                // }
                detailDisplayData.push(message)
                appendDetailBarrageOnce(message)
            }
        }, duration)
    }

    function appendDetailBarrageOnce(message) {
        $("#content").append("<li id=" + message.id + " title=" + message.text + ">" + message.text + "</li>")
    }


    // detailed basic操作
    window.basicDetailOperation = function (opera) {
        switch (opera) {
            case 'start':
                displayDetailBarrage(detailBarrageBasicSpeed)
                scrollUp(detailBarrageBasicSpeed)
                break
            case 'pause':
                clearInterval(detailIntervalId)
                clearInterval(scrollUpIntervalId)
                running = false
                break
            case 'clear':
                $("#content").empty()
                // detailBarrageData = []
                detailDisplayData = []
                break
            case 'reset':
                // 清空弹幕
                $("#content").empty()
                detailBarrageData = []
                detailDisplayData = []
                // 清空detail 定时器
                clearInterval(detailIntervalId)
                clearInterval(scrollUpIntervalId)
                // 开启定时器
                displayDetailBarrage(detailBarrageBasicSpeed)
                scrollUp(detailBarrageBasicSpeed)
                // 重启socket
                stopSocket()
                startSocket()
            default:
                console.log("detailed opera : " + opera)
        }
    };

    // detail 弹幕点击事件
    $('#content').on('click', function (event) {
        // console.log(event.target);
        let item = detailDisplayData.filter(x => x.id == event.target.id)[0]
        if (item != undefined) {
            // console.log(item)
            $("#twitter-text-p").text(item.text)
            $("#detail-author").text(item.username)
            // $("#tr-detail-dl").css('display', 'none')
            $("#tr-detail-nlp").css('display', 'table-row')
            $("#detail-nlp").text(item.sentiment == 1 ? "😍" : (item.sentiment == 0 ? "😐" : "😭"))
            $("#detail-date").text(new Date(item.createtime).toLocaleString())
            $("#detail-city").text(item.geo)
        }
    });
})

// switch 高级操作
window.switchAdvancedOperation = (function () {
    let more = false;
    return function () {
        let display = more ? 'none' : 'block';
        $('.barrage-controller').css('display', display);
        more = !more;
    }
})();

// 切换背景
window.switchBarrageBackground = (function () {
    let index = 0;
    return function () {
        let bg = '../images/barrage_bg' + index + '.png'
        // $('#barrage-canvas').css('background', bg);
        document.getElementById("barrage-canvas").style.backgroundImage = "url(" + bg + ")";
        document.getElementById("barrage-canvas").style.marginTop = '10px';
        if (index < 4)
            index += 1
        else
            index = 0
    }
})();

function getRandomInt(max) {
    return Math.floor(Math.random() * Math.floor(max));
}
