 
// IE10以下浏览器提示
function hiUpgrade() {
    window.AESKey = '';
    // 判断浏览器是否支持placeholder属性
    function isSupportPlaceholder() {
        var input = document.createElement('input');
        return 'placeholder' in input;
    };
    //判断是否是IE浏览器，包括Edge浏览器
    function IEVersion() {
        //取得浏览器的userAgent字符串
        var userAgent = navigator.userAgent;
        //判断是否IE浏览器
        var isIE = userAgent.indexOf("compatible") > -1 && userAgent.indexOf("MSIE") > -1;
        if (isIE) {
            // ie10及以下
            var reIE = new RegExp("MSIE (\\d+\\.\\d+);");
            reIE.test(userAgent);
            var fIEVersion = parseFloat(RegExp["$1"]);
            if (fIEVersion < 10 || !isSupportPlaceholder()) {
                return true;
            }
        } else {
            return false;
        }
    }
    var tpl = '<div id="hi-upgrade"><div class="hi-wrap"><p class="hi-title">无法正常浏览本网站！</p><div class="hi-close">继续浏览</div><div class="hi-text1"><p>1、您的浏览器版本过低，请升级您的浏览器。</p><p>2、如果您的浏览器是最新版本，请<span>切换到极速模式</span>访问。</p><p>3、您使用的是IE10以下的浏览器，建议您<span>使用主流浏览器</span>访问。</p></div><p class="hi-text2"><span>主流浏览器下载</span></p><ul class="hi-list"><li><a href="https://www.google.cn/intl/zh-CN/chrome/" target="_blank"><div class="hi-ico1"></div><p>谷歌浏览器</p></a></li><li><a href="http://www.firefox.com.cn/download/" target="_blank"><div class="hi-ico2"></div><p>火狐浏览器</p></a></li><li><a href="http://browser.360.cn" target="_blank"><div class="hi-ico3"></div><p>UC浏览器</p></a></li><li><a href="https://www.uc.cn" target="_blank"><div class="hi-ico4"></div><p>360浏览器</p></a></li><li><a href="https://browser.qq.com" target="_blank"><div class="hi-ico5"></div><p>QQ浏览器</p></a></li><li><a href="https://ie.sogou.com" target="_blank"><div class="hi-ico6"></div><p>搜狗浏览器</p></a></li></ul></div></div>';
    if (IEVersion()) {
        document.write(tpl);
    }
}
hiUpgrade();


//置顶
function goTop() {
	$('html,body').animate({
		'scrollTop': 0
	}, 1000);
}
// 导航
function navHead(){
    if($(window).scrollTop()>200){
        $(".hadbox").addClass("hadbox2");
        $(".rtfixed").show();
    }else{
        $(".hadbox").removeClass("hadbox2");
        $(".rtfixed").hide();
    }
}
function navFade(){
    var i = $(window).scrollTop();
    var obj = $(".hadbox");
    $(window).scroll(function(){
        if($(this).scrollTop()>i && $(this).scrollTop()>50){
            i = $(this).scrollTop();
            obj.addClass("hadbox3");
          }else{
            i = $(this).scrollTop();
            obj.removeClass("hadbox3");
          }
    });
  }
  navFade();
function navHead2(){
        if($(window).scrollTop()>200){
            // $(".hadbox").addClass("hadbox3");
            $(".rtfixed").show();
            
        }else{
            // $(".hadbox").removeClass("hadbox3");
            $(".rtfixed").hide();
        }
        $(".hadbox-sec2").height($(window).height()-$(".hadbox-sec1").height());
}

/* 头部 */
if($(".hadbox2").length!=0){
    $(".hadheight").show();
    navHead2();
    $(window).scroll(function(){
        navHead2()
    });
}else{
    navHead();
    navHead2()
    $(window).scroll(function(){
        navHead();
        navHead2()
    });
}

$(".hadbox-sec2 .container .navbox  ul  li a i").click(function(){
    if($(this).parent().parent().find("ul li").length!=0){
        $(this).parent().parent().toggleClass("active").children("ul").stop().slideToggle();
        return false;
    }
    return false;
});

$(".hadbox-sec1 .gt .basemenubut").click(function(){
    $(this).toggleClass("basemenubut2");
    $('.hadbox-sec2').stop().slideToggle();
});


$(".hadbox-sec1 .gt .f_search").click(function(){
    $(".hadbox-sec3").fadeIn();
});
$(".hadbox-sec3 .f_close").click(function(){
    $(".hadbox-sec3").fadeOut();
});
$(".hadbox-sec3").mouseleave(function(){
    $(".hadbox-sec3").fadeOut();
});


$(".hadbot .hadbot_jut .container .cont .close").click(function(){
    $(this).parents(".hadbot").removeClass("biao").stop().fadeOut();
});
/* 首页banner */
var bannerbox = new Swiper('.bannerbox',{
    speed: 800,
    watchOverflow: true, //因为仅有1个slide，swiper无效
    observer: true,  //开启动态检查器，监测swiper和slide
    observeParents: true,  //监测Swiper 的祖/父元素
    loop: $(".bannerbox .swiper-slide").length>1,
    pagination: {
        el: '.bannerbox_pagin',
        clickable :true,
    },
    on:{
        init: function(){
          swiperAnimateCache(this); //隐藏动画元素 
          swiperAnimate(this); //初始化完成开始动画
          
        }, 
        slideChangeTransitionStart: function(){ 
          swiperAnimate(this); //每个slide切换结束时也运行当前slide动画
          bannerVideoPause();
          setTimeout(function(){
            bannerVideoPlay();
          },10);
        }
    },

});
function bannerVideoPlay(){
    var obj = $(".bannerbox .swiper-slide-active video");
    if(obj.length && obj[0].readyState>3){
        obj[0].play();
    }
}
function bannerVideoPause(){
    var obj = $(".bannerbox .swiper-slide video");
    
    if(obj.length){
        obj.trigger("pause");
        obj.each(function(){
            $(this)[0].currentTime=0;
        });
        
    }
}
if($(".bannerbox .swiper-slide-active video").length){
    $(".bannerbox .swiper-slide-active video")[0].onprogress = function(){
        bannerVideoPlay();
    }
}
if($(window).width()<767){
    $(".bannerbox .swiper-slide video").remove();
}




HiSetClientHeight($(".bannerbox .swiper-slide .bag"));

/* 底部 */
$(".fotbox-sec1 .cont .lt .f_navbox>li .tit i").click(function(e){
    $(this).parents("li").toggleClass("on").siblings().removeClass("on");
    $(this).parents("li").find("ul").stop().slideToggle();
    $(this).parents("li").siblings().find("ul").stop().slideUp();
    
    e.preventDefault();
});






/* 主要 */

$(".hadbox .hadbox-sec1 .f_navbox>li").hover(
    function(){
        if($(this).find(".had_hover").length!=0){
					var _this = $(this);
					_this.find(".had_hover").stop().slideDown();
                     $(this).find(".had_hover .fz_img1").each(function(){
                var iamgesrc = $(this).data("iamgesrc");
                $(this).attr("src",iamgesrc);
            });		
        }else{
            $(this).children("ul").stop().slideDown();
            $(this).children(".f_hover1").stop().slideDown();

        }
    },
    function(){
        if($(this).find(".had_hover").length!=0){
            $(this).find(".had_hover").stop().slideUp();
        }else{
            $(this).children("ul").stop().slideUp();
            $(this).children(".f_hover1").stop().slideUp();
        }
    }
);

/* 首页js */

 HiVideoPop(".home-sec1 .cont .gt .f_icon");
 // 调用
 HiTextLyric({
    element:$(".home-sec1 .cont .lt .f_text"),//进入可视区的元素
    element2:$(".home-sec1 .cont .lt .f_text"),//执行效果的元素
    class:'on',//添加的class
    offset: 0.3,//距离可视区底部位置执行(0为底部，0.5为一半，1为顶部)
    duration:$(window).height()/2,//多远的距离执行完整个效果
});
$(".count").each(function(){
    var _ht = $(this).html();
    if(Number(_ht)){
        $(this).countUp();
    }
});
const WINDOWS_WIDTH = $(window).width();
if(WINDOWS_WIDTH>991){
    function homeProScroll(){
        //创建控制器
        var controller = new ScrollMagic.Controller();
        var offsetLeft = $(".home-sec2 .f_zhong .container .cont").width() - ($(window).width() - ($(".home-sec2 .f_zhong .container .cont").offset().left*3));
        var startOffset = $(".home-sec2").offset().top + $(".hadbox-sec1").height();
        
        var end = offsetLeft - ($(window).height() + $(".home-sec2 .f_zhong .container .cont").height());
        $(".home-sec2").height(offsetLeft);
        //创建单一动画
         var move1 = TweenMax.to($(".home-sec2 .f_zhong .container .cont"),.5, {
            marginLeft: -offsetLeft+"px",
            ease: Power0.easeIn
        });
        // 设置滚动
        var Scene1 = new ScrollMagic.Scene({
                offset: startOffset,//动画开始执行的位置
                duration: end,//滚动多远执行完整个动画，为0时会直接执行完成动画
            })
            .setTween(move1)//设置动画
            .addTo(controller);//添加到控制器
    }
    
    if($(".home-sec2").length){
        homeProScroll();
    }
}else{
    var home2_cont  = new Swiper('.home2_cont ',{
        speed: 800,
        slidesPerView : 'auto', 
        watchOverflow: true, //因为仅有1个slide，swiper无效
        observer: true,  //开启动态检查器，监测swiper和slide
        observeParents: true,  //监测Swiper 的祖/父元素
        loop: $(".home2_cont .swiper-slide").length>1,
        pagination: {
            el: '.home2_pagin', 
            clickable :true,
        },
    
    });
}
var home3_cont  = new Swiper('.home3_cont ',{
    speed: 800,
    slidesPerView : 'auto', 
    watchOverflow: true, //因为仅有1个slide，swiper无效
    observer: true,  //开启动态检查器，监测swiper和slide
    observeParents: true,  //监测Swiper 的祖/父元素
    pagination: {
        el: '.home3_pagin', 
        clickable :true,
    },
    on:{
        init: function(){
          swiperAnimateCache(this); //隐藏动画元素 
          swiperAnimate(this); //初始化完成开始动画
          
        }, 
        slideChangeTransitionStart: function(){ 
          swiperAnimate(this); //每个slide切换结束时也运行当前slide动画
        }
    },

});
var home4_cont  = new Swiper('.home4_cont ',{
    speed: 800,
    slidesPerView : 'auto', 
    // spaceBetween : 492,
    watchOverflow: true, //因为仅有1个slide，swiper无效
    observer: true,  //开启动态检查器，监测swiper和slide
    observeParents: true,  //监测Swiper 的祖/父元素
    loop: $(".home4_cont .swiper-slide").length>2,
    pagination: {
        el: '.home4_pagin', 
        clickable :true,
    },
    navigation: {
        nextEl: '.home4_next',
        // prevEl: '.swiper-button-prev',
      },
    on:{
        init: function(){
          swiperAnimateCache(this); //隐藏动画元素 
          swiperAnimate(this); //初始化完成开始动画
          
        }, 
        slideChangeTransitionStart: function(){ 
          swiperAnimate(this); //每个slide切换结束时也运行当前slide动画
        }
    },
    breakpoints: {

    }
});
/* End */

/* 新闻列表 */
// var newsbox1_nav  = new Swiper('.newsbox1_nav ',{
//     speed: 800,
//     slidesPerView : 'auto', 
//     // spaceBetween : 492,
//     watchOverflow: true, //因为仅有1个slide，swiper无效
//     observer: true,  //开启动态检查器，监测swiper和slide
//     initialSlide: $(".newsbox1_nav .swiper-slide.on").index(),
//     observeParents: true,  //监测Swiper 的祖/父元素
//     on:{
//         init: function(){
//           swiperAnimateCache(this); //隐藏动画元素 
//           swiperAnimate(this); //初始化完成开始动画
          
//         }, 
//         slideChangeTransitionStart: function(){ 
//           swiperAnimate(this); //每个slide切换结束时也运行当前slide动画
//         }
//     },
//     breakpoints: {

//     }
// });
// if(WINDOWS_WIDTH<767){
//     $(".newsbox_bag").remove();
// }

/* 加入我们 */
var joinbox3_cont = new Swiper('.joinbox3_cont',{
    slidesPerView : "auto",  
    observer: true,  //开启动态检查器，监测swiper和slide
    observeParents: true,  //监测Swiper 的祖/父元素
    watchOverflow: true, //因为仅有1个slide，swiper无效
});
var joinbox4_cont = new Swiper('.joinbox4_cont',{
    // slidesPerView : "auto",  
    spaceBetween : 50,
    observer: true,  //开启动态检查器，监测swiper和slide
    observeParents: true,  //监测Swiper 的祖/父元素
    watchOverflow: true, //因为仅有1个slide，swiper无效
    navigation: {
        nextEl: '.joinbox4_next',
        prevEl: '.joinbox4_prev',
    },
    autoplay: {
        delay: 5000,
        stopOnLastSlide: false,
        disableOnInteraction: false,
    },
    breakpoints: {
        1580: {
            spaceBetween :  40,
        },
        1280: {
            spaceBetween :  30,
        },
        767: {
            spaceBetween :  20,
        }
    }
});
var joinbox5_cont = new Swiper('.joinbox5_cont',{
    // slidesPerView : "auto",  
    speed: 800,
    spaceBetween : 30,
    observer: true,  //开启动态检查器，监测swiper和slide
    observeParents: true,  //监测Swiper 的祖/父元素
    watchOverflow: true, //因为仅有1个slide，swiper无效
    navigation: {
        nextEl: '.joinbox5_next',
        prevEl: '.joinbox5_prev',
    },
    autoplay: {
        delay: 5000,
        stopOnLastSlide: false,
        disableOnInteraction: false,
    },
    breakpoints: {
        1580: {
            spaceBetween :  20,
        },
        1280: {
            spaceBetween :  15,
        },
        767: {
            spaceBetween :  15,
        }
    }
});
var joinbox6_cont = new Swiper('.joinbox6_cont',{
    slidesPerView : "auto",  
    speed: 15000,
    spaceBetween : 20,
    loop: true,
    allowTouchMove: false,
    observer: true,  //开启动态检查器，监测swiper和slide
    observeParents: true,  //监测Swiper 的祖/父元素
    watchOverflow: true, //因为仅有1个slide，swiper无效
    autoplay: {
        delay: 0,
        stopOnLastSlide: false,
        disableOnInteraction: false,
    },
    pagination: {
        el: '.joinbox6_pagin',
        type : 'progressbar',
      },
    breakpoints: {
        1580: {
            spaceBetween :  15,
        },
        1280: {
            spaceBetween :  15,
        },
        767: {
            spaceBetween :  15,
        }
    }
});

if($(window).width()>767 && $(".joinbox_cont").length){
    function joinBoxTop(){
        var controller = new ScrollMagic.Controller();
        var _top = $(".joinbox_cont").offset().top - $(".hadbox-sec1").height() + 200;
        var end = $(window).height()/2;
        var move1 = new TimelineLite();
        move1.to($(".joinbox-sec1"),.5, {
            y: "-100vh",
            ease: Power0.easeIn
        });
        // 设置滚动
        var Scene1 = new ScrollMagic.Scene({
                offset: _top, //动画开始执行的位置
                duration: end, //滚动多远执行完整个动画，为0时会直接执行完成动画
            })
            .setTween(move1) //设置动画
            .addTo(controller); //添加到控制器
        function scrollFont(){
            var s = $(window).scrollTop();
            if(s>=_top+end + 200){
                $(".joinbox-sec2").addClass("on");
            }else{
                $(".joinbox-sec2").removeClass("on");
            }
        }
        scrollFont();
        $(window).scroll(function(){
            scrollFont();
        });
    }
    joinBoxTop();
}

/* 联系我们 */
HiAddClass($(".contactbox-sec1 .cont"),$(".contactbox-sec1 .cont"),0.2,"on",false);

/* 常见问题 */
$(document).on("click",".faqbox_list .item .f_title",function(){
    $(this).parent().toggleClass("on").siblings().removeClass("on");
    $(this).next(".f_text").stop().slideToggle().parent().siblings().find(".f_text").stop().slideUp();
});


$(document).on("click",'.public-joinlist2 .box .box2',function(){    
    if($(this).parents(".box").hasClass("on")){
        $(this).parents(".box").removeClass("on");
        $(this).siblings(".cn4").stop().slideDown();
        $(this).siblings(".cn5").stop().slideUp();
    }else{
        $(this).parents(".box").addClass("on");
        $(this).siblings(".cn4").stop().slideUp();
        $(this).siblings(".cn5").stop().slideDown();
    }
})

// 职位申请
//  触发申请弹窗
$(document).on("click",'.applybutton',function(){
    $(".shenqingggangwei").fadeIn()


    let aaazhiwei = $(this).attr("data-zhiwei")
    console.log(aaazhiwei)

    $(".public-windows99 .box .cn1 .cn3a em").text(aaazhiwei)

})


// 上传
$(document).on("change",'.public-windows99.shenqingggangwei .box10 .box11 .box12 .cn31 input',function(){
    let sFileName = $(this).val().split('\\');
        sFileName = sFileName[sFileName.length - 1];
    // oText.text(sText + sFileName);
    $(".public-windows99.shenqingggangwei .box10 .box11 .box12 .cn32 .cn34 .cn35").text(sFileName)
});

// 关闭
$(document).on("click",'.public-windows99 .box .cnclose',function(){
    $(".shenqingggangwei").fadeOut()
    $(".public-windows99.shenqingggangwei .box10 .box11 .box12 .cn31 input")[0].value = ""
    $(".public-windows99.shenqingggangwei .box10 .box11 .box12 .cn32 .cn34 .cn35").text("个人简历.docx")
})



$(document).on("click",'.public-windows99.shenqingggangwei .box10 .box11 .box12 .cn32 .zclose',function(){
    $(".public-windows99.shenqingggangwei .box10 .box11 .box12 .cn31 input")[0].value = ""
    $(".public-windows99.shenqingggangwei .box10 .box11 .box12 .cn32 .cn34 .cn35").text("个人简历.docx")
})

// 预计到岗时间
layui.use(function(){
    var laydate = layui.laydate;
    var form = layui.form;
    // 直接嵌套显示
    laydate.render({
        elem: '#laydatemorevalue',
        // value: '2023-10-25',
        isInitValue: true
    });

})
