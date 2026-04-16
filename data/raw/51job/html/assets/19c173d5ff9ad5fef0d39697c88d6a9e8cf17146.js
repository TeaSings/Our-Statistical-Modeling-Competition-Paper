let ctmid = "8063865";
let params = {
  ctmid: ctmid,
  pagesize: 999,
  pagenum: 1,
  keyword: "",
  coid: "",
  functype: "",
  jobarea: "",
};
$(".job-list").on("click", ".job-item-header", function () {
  $(this).toggleClass("active");

  // 判断是否为空
  if ($(this).next().html().trim().length === 0) {
    // 获取职位信息
    var jobid = $(this).data("id");
    coapi.getJobDetail(jobid, (res) => {
      let job = res.resultbody || {};
      let jobLink = `https://xyz.51job.com/external/apply.aspx?jobid=${job.jobid}&ctmid=${job.ctmid}`;
      let jobHtml = `
              <div class="job-desc-top">
                <div><span class="label">招聘人数：</span>${job.jobnum}</div>
                <div><span class="label">月薪：</span>${job.providesalarname}</div>
              </div>
              <div class="job-desc-cell">
                <span class="label">专业要求：</span>${job.major}
              </div>
              <div class="job-desc-cell">
                <span class="label">学历要求：</span>${job.degreefrom}
              </div>
              <div class="job-desc-cell">
                <span class="label">岗位详情：</span><br />${job.jobinfo}
              </div>

              <a href="${jobLink}" target="_blank" class="job-btn">立即投递</a>
            `;

      $(this).next().html(jobHtml);
      $(this).next().slideToggle();
    });
    return;
  }

  $(this).next().slideToggle();
});

$(".filter-wrap").on("click", ".filter-type", function () {
  $(this).siblings().removeClass("active").find(".filter-list").slideUp();

  $(this).toggleClass("active");
  $(this).find(".filter-list").slideToggle();
});

$(".filter-list").on("click", ".filter-item", function (event) {
  $(this).siblings().removeClass("active");
  $(this).toggleClass("active");
  let value = $(this).data("value");
  let type = $(this).parent().attr("id");
  let name = $(this).text()
  if ($(this).hasClass("active")) {
    params[type] = value;
  } else {
    params[type] = "";
  }

  $(this).parent().prev().text(name)
  renderJobList();
});

$(".search-btn").on("click", function () {
  params.keyword = $(".search").val();
  renderJobList();
});

$(".search").on("keyup", function (e) {
  if (e.keyCode == 13) {
    params.keyword = $(this).val();
    renderJobList();
  }
});

// 搜索器
function renderSearch() {
  coapi.getJobCondition(ctmid, function (res) {
    function sortSelect(arr, name) {
      let html = $(name).html();

      for (var i in arr) {
        html += `<div class="filter-item" data-value="${arr[i].key}">${arr[i].value}</div>`;
      }

      $(name).html(html);
    }

    sortSelect(res.resultbody.coid, "#coid");
    sortSelect(res.resultbody.jobarea, "#jobarea");
    sortSelect(res.resultbody.functype, "#functype");
  });
}
renderSearch();

// 职位渲染
function renderJobList() {
  coapi.getJobList(params, function (res) {
    let jobList = res.resultbody.joblist || [];
    let html = "";
    jobList.forEach((item) => {
      html += `<div class="job-item">
            <div class="job-item-header" data-id="${item.jobid}">
              <div class="job-name-wrap">
                <img src="./images/i8.png" alt="" class="job-icon" />
                ${item.jobname}
              </div>
              <div class="company-name">${item.coname}</div>

              <div class="job-location">${item.jobareaname}</div>
              <img src="./images/arrow.png" alt="" class="arrow" />
            </div>

            <div class="job-desc-wrap"></div>
          </div>`;
    });

    if (!html) {
      html = `<div class="no-job">暂无职位信息</div>`;
    }

    $(".job-list").html(html);
  });
}
renderJobList();
