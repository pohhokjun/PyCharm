
(function(root) {
   'use strict';

    //扩展帮助方法
    var helper = {};

    // 唯一标示 uuid
    helper.uuid = function() {
      return 'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
      });
    };
    const setCookie = function() {
        let exdate = new Date()
        var timestamp = helper.uuid();
        exdate.setDate(exdate.getDate() + 30)//天数
        document.cookie = "hingecloud" + '=' + timestamp + ';expires=' + exdate.toGMTString()
        return timestamp;
    }


    helper.setCookie = function(name, value, time) {
        if(time) {
            document.cookie = name + "=" + escape(value) + ";expires=" + time;
        } else {
            document.cookie = name + "=" + escape(value) + ";";
        }
    };
    const getCookie = function() {
        let nameData = 'hingecloud='
        let allcookies = document.cookie.split(';')
        for (let i = 0; i < allcookies.length; i++) {
            let c = allcookies[i].trim()
            if (c.indexOf(nameData) == 0) {
                return c.substring(nameData.length, c.length)
            }
        }
        return setCookie();
    }
    // 得到cookie
    helper.getCookie = function(name) {
      var arr,reg=new RegExp("(^| )"+name+"=([^;]*)(;|$)");
      if(arr= document.cookie.match(reg)){
        return unescape(arr[2]);
      } else {
          return null;
      }
    };




    //遍历
    /**
     * @method each
     * @parame loopable 要遍历的对象
     * @parame callback 回调函数
     * @parame self 上下文
     **/
     helper.each = function(loopable, callback, self) {
       	var additionalArgs = Array.prototype.slice.call(arguments,3);
       	if(loopable) {	
       	  if(loopable.length === +loopable.length) {
       	  	for(var i=0; i<loopable.length; i++) {
       	  	  callback.apply(self, [loopable[i],i].concat(additionalArgs));
       	  	}
       	  } else {
       	  	  for(var item in loopable) {
       	  	  	callback.apply(self, [loopable[item], item].concat(additionalArgs));
       	  	  }
       	  }
       	}
     };

   //扩展
   /**
    *@method extend
    *@parame base 要扩展的对象
    *@return base  返回扩展后的对象
    **/
    helper.extend = function(base) {
      helper.each(Array.prototype.slice.call(arguments, 1), function(extensionObject) {
      	helper.each(extensionObject, function(value, key) {
      	  if(extensionObject.hasOwnPrototype(key)) {
      	  	base[key] = value;
      	  }
      	});
      });
      return base;	
    };

   //返回数组元素所在的位置，确定是否包含在里面
    /**
     *@method indexOf
     *@parame arrayToSearch 查找的对象
     *@parame item 查找的元素
     *@return args  返回位置
     **/
     helper.indexOf = function(arrayToSearch,item){
       	if(Array.prototype.indexOf){
       		return arrayToSearch.indexOf(item);
       	} else {
       		for(var i=0; i< arrayToSearch.length; i++){
       			if(arrayToSearch[i] === item) return i;
       		}
       		return -1;
       	}
     };

    // 绑定事件
    helper.on = function(target, type, handler) {
       if(target.addEventListener) {
       	  target.addEventListener(type, handler, false);
       } else {
       	  target.attachEvent("on" + type,
       	  	function(event) {
       	  	   return handler.call(target, event);
       	  	}, false);
       }
    };
    
    // 请求，保存数据
    helper.send = function(obj, url) {
        var img = new Image(0, 0);
        img.src = url + obj + '&random=' +Math.random();
    };
    helper.ajax = function(obj, url) {
        const xhr = new XMLHttpRequest()
        //设置请求方法和url
        xhr.open('GET', url + obj)
        //发送
        xhr.send()
        xhr.onreadystatechange = function () {
            //判断服务端 返回的所有的结果
            if (xhr.readyState === 4) {
                //状态码 200~300之间都是成功
                if (xhr.status >= 200 && xhr.status < 300) {
                    let response = xhr.response //响应体
                    if (response != null && response != 'undefined' && response.indexOf("true")>0){
                    msg=true;
                    }
                } else {
                    console.log('server error')
                }
            }
        }
    };

  	helper.changeJSON2Query =  function (jsonObj) {
  		var args = '';
  		for (var i in jsonObj) {
  			if (args != '') {
  				args += '&';
  			}
  			args += i + '=' + encodeURIComponent(jsonObj[i]);
  		}
  		return args;
  	};

    // 监听页面所有ajax请求
    /**
     * @parames {function} callback 回调 
     */
    helper.listenAjax = function(callback) {
       var open = window.XMLHttpRequest.prototype.open;
       var send = window.XMLHttpRequest.prototype.send;

       var openReplacement = function(method,url) {
          open.apply(this, arguments);
       };

       var readystatechangeReplacement = function() {
          if(this.readyState === 4 && this.status === 200) {
            callback && callback(this);
          }
       };

       var sendReplacement = function() {
          var that = this;
          send.apply(this, arguments);
          setTimeout(function() {
             that.onreadystatechange = readystatechangeReplacement; 
          },0);
       };

       window.XMLHttpRequest.prototype.open = openReplacement;
       window.XMLHttpRequest.prototype.send = sendReplacement;
    };

    var collect = {};
    var host = window.location.host;

    collect.parmas = {};
    collect.url = '';
    collect.uuid = helper.uuid();
    var msg=false;
    collect.setParames = function() {
        var _YS = _YS || [];
        // 用户自定义收集字段
        var scriptArgs11 = document.getElementById('hingecloudstat').getAttribute('data');
		var dataType = document.getElementById('hingecloudstat').getAttribute('dataType');
        var webAppCode = document.getElementById('hingecloudstat').getAttribute('webAppCode');
        if (scriptArgs11==null || scriptArgs11=='' || scriptArgs11=='undefined'|| scriptArgs11=='null'){
            console.log("标识为空")
            return false;s
        }
        //var urls='/api/construction/';
        var urls="https://web.newland.com.cn/api/construction/";
        _YS.push(['Url',urls+'stat/getUserCount?']);
        _YS.userConfig = {
            cookie:getCookie(),
            htmlCode:scriptArgs11,
			dataType:dataType,
            webAppCode: webAppCode
        };
        _YS.syserror = [];
       if(document) {
       	  this.parmas.domain = document.domain || '';
       	  this.parmas.url = document.URL || '';
       	  this.parmas.title = document.title || '';
       	  this.parmas.referrer = document.referrer;
       }
       if(window && window.screen) {
       	  this.parmas.sh = window.screen.height || 0;
       	  this.parmas.sw = window.screen.width || 0;
       	  this.parmas.cd = window.screen.colorDepth || 0;
       }
  	   if (navigator) {
  		   this.parmas.lang = navigator.language || '';
  		   this.parmas.userAgent = navigator.userAgent || '';
  	   }
  	   if(document && document.cookie) {
  	   	   this.parmas.cookie = document.cookie;
  	   }
  	   if(!this.parmas.systemName) {
  	   	   this.parmas.systemName = host.split('.')[0] || '';
  	   }
       this.parmas.target = [];
       //解析 配置项
       if(typeof _YS != "undefined") {
       	  for(var i in _YS) {
       	  	switch(_YS[i][0]) {
       	  		case '_setAccount':
       	  		   this.parmas.accout = _YS[i][1];
       	  		   break;
       	  		case 'Action':
       	  		   this.parmas.action = _YS[i].slice(1);
       	  		   break;
       	  		case 'Target':
       	  		   this.parmas.target = _YS[i].slice(1);
       	  		   break;
       	  		case 'Url':
       	  		   collect.url = _YS[i][1];
       	  		   break;
       	  		case 'CookieBool':
       	  		   if(_YS[i][1] == 'false') {
       	  		   	 delete this.parmas.cookie;
       	  		   } 
       	  		   break;   
       	  		case 'systemName': // 指定哪个平台下的数据标记
       	  		   this.parmas.systemName = _YS[i][1];   
       	  		   break;      
       	  		default:
       	  		   break;       
       	  	}
       	  }
          if(_YS.syserror && _YS.syserror.length) {
            this.parmas.syserror = _YS.syserror;
            _YS.syserror = [];
          } else {
            delete this.parmas.syserror;
          }

          // 用户自定义字段
          if(_YS.userConfig) {
             for(var k in _YS.userConfig) {
                 if(_YS.userConfig.hasOwnProperty(k)) {
                     this.parmas[k] = _YS.userConfig[k]
                 }
             }
          }
       } else {
       	  throw "必须定义全局配置变量 _YS，配置指定的请求Url。示例： var _YS = _YS || []; _YS.push(['Url','127.0.0.1/users?']);";
       }
    };

    collect.getParames = function() {
        return this.parmas;
    };

    // 用户的停留时间
    collect.timer = function() {
    	this.disp = new Date().getTime();
    };

    collect.event = function() {
      	var that = this;
      	helper.on(document.body,'click', function(e) {
      		var $target = e.target || e.srcElement;
            var currtagName = $target.nodeName.toLowerCase();
            if(currtagName == "body" || currtagName == "html" || currtagName == "") {
               return 0;
            }
      		if(helper.indexOf(that.parmas.target, currtagName) > -1 || $target.getAttribute('collect')) {
	             if(!helper.getCookie('_YS_userAccect')) {
	                ('_YS_userAccect', that.uuid);
	                // 初次进入网站，返回用户凭证。
	                that.parmas.cookie = '_YS_userAccect='+that.uuid + ';'+ that.parmas.cookie;
	             }
	             var attrCo = $target.getAttribute('collect');
	             if(attrCo) {
	               that.parmas.collectMark = attrCo;
	             } else {
	               delete that.parmas.collectMark; 
	             }

                that.parmas.clickElement = '{nodeName:'+$target.nodeName +
                   ',title:' + $target.title + ',text:' +$target.innerHTML + '}';
      		    that.setParames();
      		    helper.send(helper.changeJSON2Query(that.getParames()), that.url);
      		}
      	});
        
        // 用户离开页面时返回逗留时间
        // window.onbeforeunload = function(evt){
        //    that.parmas.disp = new Date().getTime() - that.disp;
        //    if(!helper.getCookie('_YS_userAccect')) {
        //       helper.setCookie('_YS_userAccect', that.uuid);
        //    }
        //    delete that.parmas.collectMark;
        //    delete that.parmas.clickElement;
    	//    that.setParames();
    	//    helper.send(helper.changeJSON2Query(that.getParames()), that.url);
        // };
    };

    collect.init = function() {
       var that = this;
       /*
       helper.listenAjax(function(request) {
          var obj = {
            response: request.responseText,
            responseURL: request.responseURL
          };
          that.parmas.request = JSON.stringify(obj);
          that.setParames();
          helper.send(helper.changeJSON2Query(that.getParames()), that.url);
          delete that.parmas.request;
       });
       */
       that.timer();
       that.event();
       that.setParames();
       if(!helper.getCookie('_YS_userAccect')) {
          helper.setCookie('_YS_userAccect', that.uuid);
       }
       // helper.send(helper.changeJSON2Query(that.getParames()), that.url);
        helper.ajax(helper.changeJSON2Query(that.getParames()), that.url)
       delete that.parmas.syserror;
    };

    collect.init();

})(window);
