$(function() {
    // 微信分享
    Share();

    function Share() {
        $.support.cors = true; //
        window.IENV.useShare({
            title: "",
            desc: "",
            link: location.href,
            imgUrl: "", //图片路径
            debug: false,
        });
    }

});