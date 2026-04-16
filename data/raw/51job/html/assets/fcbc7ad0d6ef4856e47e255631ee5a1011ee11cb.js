$.extend({
    processing:function (arr,type) { //一级数据处理 arr:处理的原始数据 type：属性名称（字符串）
        var jobs= arr.reduce(function (res,itme){
            if(res[itme[type]] == null ){
               res[itme[type]] = [];
            } 
            // res[itme[type]].push({'jobname':itme.jobname,'ivstatus':itme.ivstatus})
               res[itme[type]].push(itme);
               return res;
          },{})
               return jobs;
    },
    _processing:function (Arr,name,value){ //二级数据处理 arr:处理的原始数据 name：属性名称（字符串）value：属性名称（字符串）
        var jobs=  Arr.reduce(function(res, item)  {
        if (res[item[name]] == null ) {
          res[item[name]] = {}
        }
        if (res[item[name]][item[value]] == null ) {
          res[item[name]][item[value]] = []
        }
        res[item[name]][item[value]].push(item)
        return res
      }, {})
      
        return jobs;
      },
      getUrl:function () {
        var url = decodeURI(location.search).substring(1); //获取页面地址栏参数
        var urlArr = url.split('&');
        var urlObj = {};
        for (var i = 0; i < urlArr.length; i++) {
            var urlArrItem = urlArr[i].split('=');
            urlObj[urlArrItem[0]] = urlArrItem[1]
        }
        return urlObj
      },
      unlink:function (arr) {//数组去重
         var myOrderedArray = arr.reduce(function (accumulator, currentValue) {
          if (accumulator.indexOf(currentValue) === -1) {
                accumulator.push(currentValue)
              }
              return accumulator
            }, [])
            return  myOrderedArray;
      },
      _arr_two:function (arr,num) {  //将一维数组以一定的数量拆分成二维数组
        var  len = arr.length;
        var lineNum = len % num === 0 ? len / num : Math.floor( (len / num) + 1 );
        var res = [];
        for (var i = 0; i < lineNum; i++) {
          // slice() 方法返回一个从开始到结束（不包括结束）选择的数组的一部分浅拷贝到一个新数组对象。且原始数组不会被修改。
          var temp = arr.slice(i*num, i*num+num);
          res.push(temp);
        }
        return res
      }, 
})



