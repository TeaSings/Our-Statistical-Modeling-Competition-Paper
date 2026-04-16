$(function () {
  goToTop(); //回到顶部
  function goToTop() {
    //goToTop
    jQuery.fn.goToTop = function (settings) {
      settings = jQuery.extend(
        {
          min: 1, //设置距离顶部的最小值为1
          fadeSpeed: 200, //设置一个淡出淡入的速度
          ieOffset: 50, //处理IE的兼容问题
        },
        settings
      );
      return this.each(function () {
        //listen for scroll
        var el = $(this);
        el.css("display", "none"); //in case the user forgot
        $(window).scroll(function () {
          //stupid IE hack
          if (!jQuery.support.hrefNormalized) {
            //设置这个按钮的css属性
            el.css({
              position: "absolute",
              top:
                $(window).scrollTop() + $(window).height() - settings.ieOffset,
            });
          }
          if ($(window).scrollTop() >= settings.min) {
            el.fadeIn(settings.fadeSpeed);
          } else {
            el.fadeOut(settings.fadeSpeed);
          }
        });
      });
    };
    $(function () {
      var goToTopButton =
        "<div id='goToTop'><a href='javascript:;'><img src='images/Top.png'></a></div>";
      $("#wrap").append(goToTopButton); //插入按钮的html标签
      if ($(window).scrollTop() < 1) {
        $("#goToTop").hide(); //滚动条距离顶端的距离小于showDistance是隐藏goToTop按钮
      }
      $("#goToTop").goToTop({
        min: 1,
        fadeSpeed: 500,
      });
      $("#goToTop").click(function (e) {
        e.preventDefault();
        $("html,body").animate(
          {
            scrollTop: 0,
          },
          "slow"
        );
      });
    });
  }
  // 禁止拖动img
  $("img").on("mousedown", function (e) {
    e.preventDefault();
  });
  // 点击logo回到首页
  $(".logo").click(function () {
    window.location.href = "index.html";
  });

  $(".banner").width($(window).width());

  // 微信分享
  Share();
  function Share() {
    $.support.cors = true; //
    window.IENV.useShare({
      title: document.title,
      desc: "与宝武一起创启未来",
      link: location.href,
      imgUrl: "images/share.jpg", //图片路径
      debug: false,
    });
  }
});
