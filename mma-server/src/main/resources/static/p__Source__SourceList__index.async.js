(self.webpackChunk=self.webpackChunk||[]).push([[951],{43758:function(B,_,a){"use strict";a.d(_,{L:function(){return u},h:function(){return h}});var h="/sources",u="/sourcesNew"},7842:function(B,_,a){"use strict";a.r(_),a.d(_,{default:function(){return R}});var h=a(66933),u=a.n(h),E=a(85067),s=a(15999),l=a(71577),O=a(8836),P=a(18036),v=a(62435),U=a(29883),N=a.n(U),x=a(94043),I=a.n(x),$=a(50146),W=a(76772),D=a(54458),p=a(86074),L=function(d){var j=d==null?void 0:d.dataSource,w=(0,v.useState)(0),S=u()(w,2),T=S[0],y=S[1],k=(0,v.useState)(""),z=u()(k,2),g=z[0],f=z[1],m=(0,v.useState)(!1),b=u()(m,2),C=b[0],r=b[1],t=j==null?void 0:j.id,n=function(){d.onCancel(),r(!1)};return(0,v.useEffect)(function(){t!=null&&(r(!1),f(""),y(0),(0,O.v0)(t).then(N()(I().mark(function e(){var i;return I().wrap(function(o){for(;;)switch(o.prev=o.next){case 0:i=setInterval(N()(I().mark(function Z(){var K,M;return I().wrap(function(G){for(;;)switch(G.prev=G.next){case 0:return G.next=2,(0,O.jp)(t);case 2:K=G.sent,K.success?(M=K.progress,M>0&&y(M),(M>=100||M<0)&&(clearInterval(i),r(!0),y(100))):(f(K.message),r(!0),clearInterval(i));case 4:case"end":return G.stop()}},Z)})),2e3);case 1:case"end":return o.stop()}},e)}))).catch(function(e){var i=e.response.data;r(!0),f(i.message)}))},[t]),(0,p.jsxs)($.Z,{title:'"'.concat(j==null?void 0:j.name,'"\u5143\u6570\u636E\u66F4\u65B0\u8FDB\u5EA6'),visible:d.visible,closable:C,keyboard:!1,maskClosable:!1,destroyOnClose:!0,footer:null,onCancel:n,children:[g===""?"":(0,p.jsx)(W.Z,{message:g,type:"error",showIcon:!0}),(0,p.jsx)(D.Z,{percent:T,status:g===""?"active":"exception"})]})},A=a(43758),R=function(){var F=(0,v.useRef)(),d=(0,v.useState)(!1),j=u()(d,2),w=j[0],S=j[1],T=(0,v.useState)(),y=u()(T,2),k=y[0],z=y[1],g=[{title:"\u6570\u636E\u6E90\u540D",dataIndex:"name",render:function(m,b){return(0,p.jsx)(P.Link,{to:A.h+"/"+b.name,children:m})},formItemProps:{lightProps:{labelFormatter:function(m){return"app-".concat(m)}}}},{title:"\u7C7B\u578B",dataIndex:"type"},{title:"db\u6570",dataIndex:"dbNum"},{title:"table\u6570",dataIndex:"tableNum"},{title:"partition\u6570",dataIndex:"partitionNum"},{title:"\u6700\u65B0\u66F4\u65B0",dataIndex:"lastUpdateTime",valueType:"dateTime"},{title:"\u64CD\u4F5C",width:"164px",key:"option",valueType:"option",render:function(m,b){return[(0,p.jsx)("a",{onClick:function(){z(b),S(!0)},children:"\u66F4\u65B0"},"update")]}}];return(0,p.jsxs)(E._z,{children:[(0,p.jsx)(s.Z,{columns:g,actionRef:F,request:function(m,b,C){return(0,O.e6)()},rowKey:"id",search:!1,toolbar:{actions:[(0,p.jsx)(P.Link,{type:"primary",to:A.L,children:(0,p.jsx)(l.Z,{type:"primary",children:"\u6DFB\u52A0\u6570\u636E\u6E90"})},"key")]}}),(0,p.jsx)(L,{dataSource:k,visible:w,onCancel:function(){S(!1)}})]})}},8836:function(B,_,a){"use strict";a.d(_,{Ih:function(){return p},Lk:function(){return z},S6:function(){return A},Sl:function(){return j},Ui:function(){return y},Xn:function(){return f},b5:function(){return N},e6:function(){return O},fd:function(){return F},j1:function(){return W},jp:function(){return S},v0:function(){return I},xh:function(){return v}});var h=a(29883),u=a.n(h),E=a(94043),s=a.n(E),l=a(18036);function O(){return P.apply(this,arguments)}function P(){return P=u()(s().mark(function r(){return s().wrap(function(n){for(;;)switch(n.prev=n.next){case 0:return n.abrupt("return",(0,l.request)("/api/sources",{method:"GET"}));case 1:case"end":return n.stop()}},r)})),P.apply(this,arguments)}function v(r,t){return U.apply(this,arguments)}function U(){return U=u()(s().mark(function r(t,n){var e;return s().wrap(function(c){for(;;)switch(c.prev=c.next){case 0:return e="/api/sources/byName?name=".concat(t),n&&(e="".concat(e,"&config=1")),c.abrupt("return",(0,l.request)(e,{method:"GET"}));case 3:case"end":return c.stop()}},r)})),U.apply(this,arguments)}function N(r){return x.apply(this,arguments)}function x(){return x=u()(s().mark(function r(t){return s().wrap(function(e){for(;;)switch(e.prev=e.next){case 0:return e.abrupt("return",(0,l.request)("/api/sources/"+t,{method:"GET"}));case 1:case"end":return e.stop()}},r)})),x.apply(this,arguments)}function I(r){return $.apply(this,arguments)}function $(){return $=u()(s().mark(function r(t){return s().wrap(function(e){for(;;)switch(e.prev=e.next){case 0:return e.abrupt("return",(0,l.request)("/api/sources/"+t+"/update",{method:"PUT"}));case 1:case"end":return e.stop()}},r)})),$.apply(this,arguments)}function W(r){return D.apply(this,arguments)}function D(){return D=u()(s().mark(function r(t){return s().wrap(function(e){for(;;)switch(e.prev=e.next){case 0:return e.abrupt("return",(0,l.request)("/api/sources",{method:"POST",headers:{"Content-Type":"application/json"},data:t}));case 1:case"end":return e.stop()}},r)})),D.apply(this,arguments)}function p(r,t){return L.apply(this,arguments)}function L(){return L=u()(s().mark(function r(t,n){return s().wrap(function(i){for(;;)switch(i.prev=i.next){case 0:return i.abrupt("return",(0,l.request)("/api/sources/".concat(t),{method:"PUT",headers:{"Content-Type":"application/json"},data:n}));case 1:case"end":return i.stop()}},r)})),L.apply(this,arguments)}function A(r){return R.apply(this,arguments)}function R(){return R=u()(s().mark(function r(t){var n,e;return s().wrap(function(c){for(;;)switch(c.prev=c.next){case 0:return c.next=2,(0,l.request)("/api/sources/".concat(t,"/config"),{method:"GET"});case 2:return e=c.sent,(n=e.data)===null||n===void 0||n.forEach(function(o){o.required===!0?o.desc+=" (\u5FC5\u586B)":o.desc+=" (\u53EF\u9009)"}),c.abrupt("return",e);case 5:case"end":return c.stop()}},r)})),R.apply(this,arguments)}function F(r){return d.apply(this,arguments)}function d(){return d=u()(s().mark(function r(t){return s().wrap(function(e){for(;;)switch(e.prev=e.next){case 0:return e.abrupt("return",(0,l.request)("/api/sources/items/?type=".concat(t),{method:"GET"}));case 1:case"end":return e.stop()}},r)})),d.apply(this,arguments)}function j(){return w.apply(this,arguments)}function w(){return w=u()(s().mark(function r(){return s().wrap(function(n){for(;;)switch(n.prev=n.next){case 0:return n.abrupt("return",(0,l.request)("/api/sources/types",{method:"GET"}));case 1:case"end":return n.stop()}},r)})),w.apply(this,arguments)}function S(r){return T.apply(this,arguments)}function T(){return T=u()(s().mark(function r(t){return s().wrap(function(e){for(;;)switch(e.prev=e.next){case 0:return e.abrupt("return",(0,l.request)("/api/sources/"+t+"/progress",{method:"GET"}));case 1:case"end":return e.stop()}},r)})),T.apply(this,arguments)}function y(r,t,n){return k.apply(this,arguments)}function k(){return k=u()(s().mark(function r(t,n,e){var i;return s().wrap(function(o){for(;;)switch(o.prev=o.next){case 0:return i=Object.assign({},t),i.sorter=n,o.abrupt("return",(0,l.request)("/api/dbs",{method:"PUT",headers:{"Content-Type":"application/json"},data:i}));case 3:case"end":return o.stop()}},r)})),k.apply(this,arguments)}function z(r,t,n){return g.apply(this,arguments)}function g(){return g=u()(s().mark(function r(t,n,e){var i;return s().wrap(function(o){for(;;)switch(o.prev=o.next){case 0:return i=Object.assign({},t),i.sorter=n,o.abrupt("return",(0,l.request)("/api/tables",{method:"PUT",headers:{"Content-Type":"application/json"},data:i}));case 3:case"end":return o.stop()}},r)})),g.apply(this,arguments)}function f(r,t,n){return m.apply(this,arguments)}function m(){return m=u()(s().mark(function r(t,n,e){var i;return s().wrap(function(o){for(;;)switch(o.prev=o.next){case 0:return i=Object.assign({},t),i.sorter=n,o.abrupt("return",(0,l.request)("/api/partitions",{method:"PUT",headers:{"Content-Type":"application/json"},data:i}));case 3:case"end":return o.stop()}},r)})),m.apply(this,arguments)}function b(r){return C.apply(this,arguments)}function C(){return C=_asyncToGenerator(_regeneratorRuntime.mark(function r(t){return _regeneratorRuntime.wrap(function(e){for(;;)switch(e.prev=e.next){case 0:return e.abrupt("return",request("/api/partitions/status",{method:"PUT",headers:{"Content-Type":"application/json"},data:{ptIds:t}}));case 1:case"end":return e.stop()}},r)})),C.apply(this,arguments)}},46700:function(B,_,a){var h={"./af":42786,"./af.js":42786,"./ar":30867,"./ar-dz":14130,"./ar-dz.js":14130,"./ar-kw":96135,"./ar-kw.js":96135,"./ar-ly":56440,"./ar-ly.js":56440,"./ar-ma":47702,"./ar-ma.js":47702,"./ar-sa":16040,"./ar-sa.js":16040,"./ar-tn":37100,"./ar-tn.js":37100,"./ar.js":30867,"./az":31083,"./az.js":31083,"./be":9808,"./be.js":9808,"./bg":68338,"./bg.js":68338,"./bm":67438,"./bm.js":67438,"./bn":8905,"./bn-bd":76225,"./bn-bd.js":76225,"./bn.js":8905,"./bo":11560,"./bo.js":11560,"./br":1278,"./br.js":1278,"./bs":80622,"./bs.js":80622,"./ca":2468,"./ca.js":2468,"./cs":5822,"./cs.js":5822,"./cv":50877,"./cv.js":50877,"./cy":47373,"./cy.js":47373,"./da":24780,"./da.js":24780,"./de":59740,"./de-at":60217,"./de-at.js":60217,"./de-ch":60894,"./de-ch.js":60894,"./de.js":59740,"./dv":5300,"./dv.js":5300,"./el":50837,"./el.js":50837,"./en-au":78348,"./en-au.js":78348,"./en-ca":77925,"./en-ca.js":77925,"./en-gb":22243,"./en-gb.js":22243,"./en-ie":46436,"./en-ie.js":46436,"./en-il":47207,"./en-il.js":47207,"./en-in":44175,"./en-in.js":44175,"./en-nz":76319,"./en-nz.js":76319,"./en-sg":31662,"./en-sg.js":31662,"./eo":92915,"./eo.js":92915,"./es":55655,"./es-do":55251,"./es-do.js":55251,"./es-mx":96112,"./es-mx.js":96112,"./es-us":71146,"./es-us.js":71146,"./es.js":55655,"./et":5603,"./et.js":5603,"./eu":77763,"./eu.js":77763,"./fa":76959,"./fa.js":76959,"./fi":11897,"./fi.js":11897,"./fil":42549,"./fil.js":42549,"./fo":94694,"./fo.js":94694,"./fr":94470,"./fr-ca":63049,"./fr-ca.js":63049,"./fr-ch":52330,"./fr-ch.js":52330,"./fr.js":94470,"./fy":5044,"./fy.js":5044,"./ga":29295,"./ga.js":29295,"./gd":2101,"./gd.js":2101,"./gl":38794,"./gl.js":38794,"./gom-deva":27884,"./gom-deva.js":27884,"./gom-latn":23168,"./gom-latn.js":23168,"./gu":95349,"./gu.js":95349,"./he":24206,"./he.js":24206,"./hi":30094,"./hi.js":30094,"./hr":30316,"./hr.js":30316,"./hu":22138,"./hu.js":22138,"./hy-am":11423,"./hy-am.js":11423,"./id":29218,"./id.js":29218,"./is":90135,"./is.js":90135,"./it":90626,"./it-ch":10150,"./it-ch.js":10150,"./it.js":90626,"./ja":39183,"./ja.js":39183,"./jv":24286,"./jv.js":24286,"./ka":12105,"./ka.js":12105,"./kk":47772,"./kk.js":47772,"./km":18758,"./km.js":18758,"./kn":79282,"./kn.js":79282,"./ko":33730,"./ko.js":33730,"./ku":1408,"./ku.js":1408,"./ky":33291,"./ky.js":33291,"./lb":36841,"./lb.js":36841,"./lo":55466,"./lo.js":55466,"./lt":57010,"./lt.js":57010,"./lv":37595,"./lv.js":37595,"./me":39861,"./me.js":39861,"./mi":35493,"./mi.js":35493,"./mk":95966,"./mk.js":95966,"./ml":87341,"./ml.js":87341,"./mn":5115,"./mn.js":5115,"./mr":10370,"./mr.js":10370,"./ms":9847,"./ms-my":41237,"./ms-my.js":41237,"./ms.js":9847,"./mt":72126,"./mt.js":72126,"./my":56165,"./my.js":56165,"./nb":64924,"./nb.js":64924,"./ne":16744,"./ne.js":16744,"./nl":93901,"./nl-be":59814,"./nl-be.js":59814,"./nl.js":93901,"./nn":83877,"./nn.js":83877,"./oc-lnc":92135,"./oc-lnc.js":92135,"./pa-in":15858,"./pa-in.js":15858,"./pl":64495,"./pl.js":64495,"./pt":89520,"./pt-br":57971,"./pt-br.js":57971,"./pt.js":89520,"./ro":96459,"./ro.js":96459,"./ru":21793,"./ru.js":21793,"./sd":40950,"./sd.js":40950,"./se":37930,"./se.js":37930,"./si":90124,"./si.js":90124,"./sk":64249,"./sk.js":64249,"./sl":14985,"./sl.js":14985,"./sq":51104,"./sq.js":51104,"./sr":49131,"./sr-cyrl":79915,"./sr-cyrl.js":79915,"./sr.js":49131,"./ss":85893,"./ss.js":85893,"./sv":98760,"./sv.js":98760,"./sw":91172,"./sw.js":91172,"./ta":27333,"./ta.js":27333,"./te":23110,"./te.js":23110,"./tet":52095,"./tet.js":52095,"./tg":27321,"./tg.js":27321,"./th":9041,"./th.js":9041,"./tk":19005,"./tk.js":19005,"./tl-ph":75768,"./tl-ph.js":75768,"./tlh":89444,"./tlh.js":89444,"./tr":72397,"./tr.js":72397,"./tzl":28254,"./tzl.js":28254,"./tzm":51106,"./tzm-latn":30699,"./tzm-latn.js":30699,"./tzm.js":51106,"./ug-cn":9288,"./ug-cn.js":9288,"./uk":67691,"./uk.js":67691,"./ur":13795,"./ur.js":13795,"./uz":6791,"./uz-latn":60588,"./uz-latn.js":60588,"./uz.js":6791,"./vi":65666,"./vi.js":65666,"./x-pseudo":14378,"./x-pseudo.js":14378,"./yo":75805,"./yo.js":75805,"./zh-cn":83839,"./zh-cn.js":83839,"./zh-hk":55726,"./zh-hk.js":55726,"./zh-mo":99807,"./zh-mo.js":99807,"./zh-tw":74152,"./zh-tw.js":74152};function u(s){var l=E(s);return a(l)}function E(s){if(!a.o(h,s)){var l=new Error("Cannot find module '"+s+"'");throw l.code="MODULE_NOT_FOUND",l}return h[s]}u.keys=function(){return Object.keys(h)},u.resolve=E,B.exports=u,u.id=46700}}]);
