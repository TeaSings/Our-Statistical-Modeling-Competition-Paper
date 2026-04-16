var pp = new pCpT({
  ctmid: "6780974", //会员号 必填
  joblistDiv: ".info-ul", // 职位列表容器
  pagesize: 1000, //每页数量 最大100
  functype: "#functype3", //搜索 公司
  degreefrom: "#functype4", //搜索学历
  keyword: "#keyword", //搜索 关键字
  seachBtn: ".fd", //搜索按钮
  divid: "#functype2",
  count: ".count", //总职位数存放位置
  countnum: "", //总职位数
  totalpage: ".total", //总职位页数存放位置
  totalpagenum: "", //总职位页数
  presentpage: ".present", //当前页数
  firstBtn: ".first", //第一页按钮
  finalBtn: ".final", //末页按钮
  preBtn: ".pre", //上一页按钮
  nextBtn: ".next", //下一页按钮
  Jumppage: "#inputpage", //跳转输入框
  goBtn: "#go", //点击跳转按钮
});
// 中层招聘—01、劳务派遣—02、社会招聘—03
function pCpT(obj) {
  // 接口参数
  var params = {
    ctmid: obj.ctmid, //会员号 支持多选
    pagesize: "", //每页数选填  最多一页100条
    pagenum: "", //第几页 最大500页
    functype: "",
    jobarea: "",
    keyword: "",
    functype: "",
    industrytype: "",
    salary: "",
    degreefrom: "",
    sort: "joborder", //职位排序
    sequence: 0, //1：正序;0：倒序;
    // poscode: 'A,C',
    divid: "",
    poscode: "3",
  };
  let sdata = [];
  let sdata1 = [];
  let shDivid = [];
  // let dividArrOrder = [
  //   { divname: '总行金融市场部', order: '1' },
  //   { divname: '总行零售金融部', order: '2' },
  //   { divname: '总行网络金融部', order: '3' },
  //   { divname: '总行授信管理部', order: '4' },
  //   { divname: '总行风险管理部', order: '5' },
  //   { divname: '总行营业部', order: '6' },
  //   { divname: '长沙分行', order: '7' },
  //   { divname: '株洲分行', order: '8' },
  //   { divname: '衡阳分行', order: '9' },
  //   { divname: '怀化分行', order: '10' },
  //   { divname: '常德分行', order: '11' },
  //   { divname: '郴州分行', order: '12' },
  //   { divname: '益阳分行', order: '13' },
  //   { divname: '张家界分行', order: '14' },
  //   { divname: '湘西分行', order: '15' }
  // ]
  let dividBD = [

    // "总行公司业务部",
    // "总行金融市场部",
    // "总行交易银行部",
    // "总行零售业务部",
    // "总行私人银行部",
    // "总行授信管理部",
    // "总行金融科技部",
    // "总行审计部",
    // "总行保卫部",
    // "总行营业部",
    // "长沙分行",
    // "湘江新区分行",
    // "株洲分行",
    // "湘潭分行",
    // "衡阳分行",
    // "岳阳分行",
    // "邵阳分行",
    // "怀化分行",
    // "常德分行",
    // "郴州分行",
    // "益阳分行",
    // "娄底分行",
    // "永州分行",
    // "张家界分行",
    // "湘西分行",

    "宁乡支行",
    "望城支行",
    "星沙支行",
    "浏阳支行",




  ];
  let isDiv = true;
  // 职位列表
  function getJobList(params) {
    //职位列表接口
    params.pagesize = obj.pagesize; //每页数选填  最多一页100条
    // params.pagenum = $(obj.presentpage).text() //第几页 最大500页
    coapi.getJobList(params, function (data) {
      sdata1 = [];
      sdata = [];
      //调用职位列表接口
      var html = "";
      if (
        data.resultbody.length !== 0 &&
        data.resultbody.joblist.length !== 0
      ) {
        for (var i = 0; i < data.resultbody.joblist.length; i++) {
          sdata1.push(data.resultbody.joblist[i]);
        }
      }
      // 部门排序
      const orderMap = Object.fromEntries(
        dividBD.map((name, index) => [name, index])
      );

     // 排序函数：按 jobid 从小到大
      sdata = sdata1.sort((a, b) => {
        const jobIdA = a && a.jobid;
        const jobIdB = b && b.jobid;
        const nA = Number(jobIdA);
        const nB = Number(jobIdB);
        if (Number.isNaN(nA) || Number.isNaN(nB)) {
          // jobid 如果不是纯数字，退回字符串比较
          return String(jobIdA).localeCompare(String(jobIdB));
        }
        return nA - nB;
      });
      console.log('sdata--',sdata);
      
      if (sdata.length != 0) {
        if (sdata.length > $(obj.presentpage).text() * 10) {
          for (
            var i = ($(obj.presentpage).text() - 1) * 10;
            i < $(obj.presentpage).text() * 10;
            i++
          ) {
            html += `<li>
                  <div class="top flex-row j-s-b a-c" flag="0" data-id = ${sdata[i].jobid}>
                      <div>
                          <img src="./img/top1.png" alt="">
                          ${sdata[i].jobname}
                      </div>
                      <div class="dj" >
                          查看详情
                          <img src="./img/dj.png" alt="">
                      </div>
                  </div>
                  <div class="btm">
                  </div>
              </li>
              `;
          }
        } else {
          for (
            var i = ($(obj.presentpage).text() - 1) * 10;
            i < sdata.length;
            i++
          ) {
            html += `
              <li>
                  <div class="top flex-row j-s-b a-c" flag="0" data-id = ${sdata[i].jobid}>
                      <div>
                          <img src="./img/top1.png" alt="">
                          ${sdata[i].jobname}
                      </div>
                      <div class="dj" >
                          查看详情
                          <img src="./img/dj.png" alt="">
                      </div>
                  </div>
                  <div class="btm">
                  </div>
              </li>
              `;
          }
        }
      } else {
        html = `
          <li><p class="buzai">您搜索的结果不存在</p></li>
       `;
      }
      $(obj.joblistDiv).html(html);
      obj.countnum = sdata.length;
      obj.totalpagenum = Math.ceil(obj.countnum / 10);
      // obj.countnum = data.resultbody.totalnum
      // obj.countnum = sdata.length
      // obj.totalpagenum = Math.ceil(data.resultbody.totalnum / obj.pagesize)
      // obj.totalpagenum = Math.ceil(obj.countnum / 10)
      //    改动
      if ($(obj.joblistDiv).find("td").text() == "您搜索的结果不存在") {
        $(obj.count).text(0);
        $(obj.totalpage).text(0);
        $(obj.presentpage).text(0);
      } else {
        // $(obj.count).text(data.resultbody.totalnum)
        $(obj.count).text(obj.countnum);
        $(obj.totalpage).text(obj.totalpagenum);
      }
      //    改动   END
      //初始化功能函数
      $(".top").click(function () {
        const jobid = $(this).attr("data-id");
        // console.log(jobid);
        let _this = this;
        coapi.getJobDetail(jobid, function (data) {
          //取到数据之后的操作
          var detailHtml = "";
          const jobDetail = data.resultbody;
          // console.log(jobDetail);
          jobDetail.jobinfo = jobDetail.jobinfo.replace(
            /岗位职责：/g,
            '<p class="pt pt2">岗位职责：</p>'
          );
          jobDetail.jobinfo = jobDetail.jobinfo.replace(
            /应聘条件：/g,
            '<p class="pt pt2">应聘条件：</p>'
          );
          detailHtml += `
                <div class="t1 a-c">
                    ${jobDetail.jobinfo}
                </div>
               
                <div class="btn">
                    <a href="https://jobs.51job.com/all/${jobid}.html">
                        <img src="./img/btn.png" alt="">
                    </a>
                </div>
                        `;
          $(_this).next().html(detailHtml);
          $(_this).next().toggle();
        });
      });
    });
  }
  getJobList(params); //调用职位列表接口函数
  function combineKey(data) {
    let combineData = [];
    //遍历data,提取key前4位并保存
    $(data).each(function () {
      combineData.push({
        k: this.key.substring(0, 4),
        value: this.value,
        key: this.key,
      });
    });
    //遍历combineData,过滤省和特殊行政区
    $(combineData).each(function () {
      let item = this;
      let keys = [];
      if (
        item.value.indexOf("--") === -1 &&
        item.value.indexOf("省") === -1 &&
        item.value.indexOf("区") === -1
      ) {
        if (
          item.value.replace(/(-)/gm, "") !== "北京" ||
          item.value.replace(/(-)/gm, "") !== "上海" ||
          item.value.replace(/(-)/gm, "") !== "广州" ||
          item.value.replace(/(-)/gm, "") !== "深圳"
        ) {
          //当前市的key与下级区key拼接
          for (let i in combineData) {
            if (combineData[i].k === item.key.substring(0, 4)) {
              keys.push(combineData[i].key);
            }
          }
          item.key = keys.join(",");
        }
      }
    });
    return combineData;
  }
  getJobCon(params); //调用搜索器接口函数
  function getJobCon(params) {
    //搜索器函数
    coapi.getJobCondition(
      obj.ctmid,
      function (data) {
        for (var i = 0; i < data.resultbody.functype.length; i++) {
          if (
            data.resultbody.functype[i].key !== "1F00" &&
            data.resultbody.functype[i].key !== "8700"
          ) {
            $(obj.functype).append(
              '<option value="' +
              data.resultbody.functype[i].key +
              '">' +
              data.resultbody.functype[i].value +
              "</option>"
            );
          }
        }
        // const filteredData = data.resultbody.divid.filter(
        //   (item) => !(item.value === "长沙地区分行" && item.divorder === "998")
        // );
        var divid = data.resultbody.divid;
        // var divid = data.resultbody.divid
        divid.sort(function (a, b) {
          return b.divorder - a.divorder;
        });
        // console.log(divid);
        for (const k in dividBD) {
          for (var i = 0; i < divid.length; i++) {
            // console.log(dividBD[k]==divid[i].value);
            if (dividBD[k] == divid[i].value) {
              $(obj.divid).append(
                '<option value="' +
                divid[i].key +
                '">' +
                divid[i].value +
                "</option>"
              );
            }
          }
        }
        var city = combineKey(data.resultbody.jobarea);
        $.each(city, function (i, item) {
          if (
            (item.value !== "湖南省") &
            (item.value !== "--天心区") &
            (item.value !== "--岳麓区") &
            (item.value !== "--长沙县") &
            (item.value !== "--宁乡市") &
            (item.value !== "--浏阳市")
          ) {
            $(obj.jobarea).append(
              '<option value="' + item.key + '">' + item.value + "</option>"
            );
          }
        });
      },
      params
    );
  }
  $(obj.degreefrom).change(function (event) {
    //学历
    event.stopPropagation();
    params.degreefrom = $(obj.degreefrom).val();
    // console.log(params);
    $(obj.presentpage).text("1");
    sdata = [];
    getJobList(params); //调用职位列表接口函数
  });
  $(obj.functype).change(function (event) {
    //职能
    event.stopPropagation();
    params.functype = $(obj.functype).val();
    $(obj.presentpage).text("1");
    sdata = [];
    getJobList(params); //调用职位列表接口函数
  });
  $(obj.divid).change(function (event) {
    //职能
    event.stopPropagation();
    params.divid = $(obj.divid).val();
    $(obj.presentpage).text("1");
    sdata = [];
    getJobList(params); //调用职位列表接口函数
  });
  $(obj.seachBtn).click(function () {
    //搜索
    event.stopPropagation();
    params.keyword = $(obj.keyword).val();
    $(obj.presentpage).text("1");
    sdata = [];
    getJobList(params); //调用职位列表接口函数
  });
  //  改动
  $(obj.keyword).focus(function () {
    //关键字
    $(obj.keyword).val("");
  });
  $(obj.keyword).bind("keypress", function (event) {
    //关键字
    if (event.keyCode == "13") {
      params.keyword = $(obj.keyword).val();
      $(obj.presentpage).text("1");
      getJobList(params); //调用职位列表接口函数
    }
  });
  // 分页
  $(obj.preBtn).click(function (event) {
    //上一页
    event.stopPropagation();
    if (parseInt($(obj.presentpage).text()) > 1) {
      $(obj.presentpage).text(parseInt($(obj.presentpage).text()) - 1);
      getJobList(params); //调用职位列表接口函数
    }
  });
  $(obj.nextBtn).click(function (event) {
    //下一页
    /* Act on the event */
    event.stopPropagation();
    var x = parseInt($(obj.presentpage).text());
    if (parseInt($(obj.presentpage).text()) < obj.totalpagenum) {
      $(obj.presentpage).text(x + 1);
      getJobList(params); //调用职位列表接口函数
    }
  });
  $(obj.firstBtn).click(function (event) {
    //首页
    /* Act on the event */
    event.stopPropagation();
    $(obj.presentpage).text(1);
    getJobList(params); //调用职位列表接口函数
  });
  $(obj.finalBtn).click(function (event) {
    //尾页
    /* Act on the event */
    event.stopPropagation();
    $(obj.presentpage).text(parseInt($(obj.totalpage).text()));
    getJobList(params); //调用职位列表接口函数
  });
  $(obj.goBtn).click(function (event) {
    //跳转
    /* Act on the event */
    event.stopPropagation();
    if (
      parseInt($(obj.Jumppage).val()) > 0 &&
      parseInt($(obj.Jumppage).val()) <= parseInt($(obj.totalpage).text()) &&
      $(obj.Jumppage).val() !== ""
    ) {
      //             改动
      parseInt(
        $(obj.presentpage).text(
          $(obj.Jumppage)
            .val()
            .replace(/\b(0+)/gi, "")
        )
      );
      $(obj.Jumppage).val("");
      getJobList(params); //调用职位列表接口函数
    }
  });
  $(obj.Jumppage).bind("keypress", function (event) {
    //input跳转
    if (event.keyCode == "13") {
      if (
        parseInt($(obj.Jumppage).val()) > 0 &&
        parseInt($(obj.Jumppage).val()) <= parseInt($(obj.totalpage).text()) &&
        $(obj.Jumppage).val() !== ""
      ) {
        parseInt(
          $(obj.presentpage).text(
            $(obj.Jumppage)
              .val()
              .replace(/\b(0+)/gi, "")
          )
        );
        $(obj.Jumppage).val("");
        getJobList(params); //调用职位列表接口函数
      }
    }
  });
}
