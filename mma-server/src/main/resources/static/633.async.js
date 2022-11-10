"use strict";(self.webpackChunk=self.webpackChunk||[]).push([[633],{32633:function(Ae,ne,n){n.d(ne,{Z:function(){return Pe}});var te=n(29883),re=n.n(te),ae=n(37306),N=n.n(ae),le=n(66933),W=n.n(le),oe=n(94043),U=n.n(oe),Me=n(61814),ie=n(86835),Je=n(96177),O=n(71577),ue=n(4942),de=n(29439),Y=n(44925),z=n(74165),ve=n(15861),v=n(1413),f=n(86074),se=n(49101),fe=n(97462),ce=n(78775),he=n(26369),Q=n(22270),me=n(48171),ge=n(60249),ye=n(56746),Ce=n(21770),Re=n(88306),X=n(8880),R=n(62435),Ze=n(15999),be=["onTableChange","maxLength","formItemProps","recordCreatorProps","rowKey","controlled","defaultValue","onChange","editableFormRef"],Te=["record","position","creatorButtonText","newRecordType","parentKey","style"],k=R.createContext(void 0);function q(e){var C=e.children,T=e.record,D=e.position,j=e.newRecordType,S=e.parentKey,F=(0,R.useContext)(k);return R.cloneElement(C,(0,v.Z)((0,v.Z)({},C.props),{},{onClick:function(){var K=(0,ve.Z)((0,z.Z)().mark(function w(E){var P,Z,u,a;return(0,z.Z)().wrap(function(t){for(;;)switch(t.prev=t.next){case 0:return t.next=2,(P=(Z=C.props).onClick)===null||P===void 0?void 0:P.call(Z,E);case 2:if(a=t.sent,a!==!1){t.next=5;break}return t.abrupt("return");case 5:F==null||(u=F.current)===null||u===void 0||u.addEditRecord(T,{position:D,newRecordType:j,parentKey:S});case 6:case"end":return t.stop()}},w)}));function V(w){return K.apply(this,arguments)}return V}()}))}function p(e){var C,T,D=(0,ce.YB)(),j=e.onTableChange,S=e.maxLength,F=e.formItemProps,K=e.recordCreatorProps,V=e.rowKey,w=e.controlled,E=e.defaultValue,P=e.onChange,Z=e.editableFormRef,u=(0,Y.Z)(e,be),a=(0,he.Z)(e.value),m=(0,R.useRef)(),t=(0,R.useRef)();(0,R.useImperativeHandle)(u.actionRef,function(){return m.current});var b=(0,Ce.Z)(function(){return e.value||E||[]},{value:e.value,onChange:e.onChange}),c=(0,de.Z)(b,2),r=c[0],M=c[1],s=R.useMemo(function(){return typeof V=="function"?V:function(g,l){return g[V]||l}},[V]),I=function(l){if(typeof l=="number"&&!e.name){if(l>=r.length)return l;var o=r&&r[l];return s==null?void 0:s(o,l)}if((typeof l=="string"||l>=r.length)&&e.name){var i=r.findIndex(function(d,h){var y;return(s==null||(y=s(d,h))===null||y===void 0?void 0:y.toString())===(l==null?void 0:l.toString())});return i}return l};(0,R.useImperativeHandle)(Z,function(){var g=function(i){var d,h;if(i==null)throw new Error("rowIndex is required");var y=I(i),x=[e.name,(d=y==null?void 0:y.toString())!==null&&d!==void 0?d:""].flat(1).filter(Boolean);return(h=t.current)===null||h===void 0?void 0:h.getFieldValue(x)},l=function(){var i,d=[e.name].flat(1).filter(Boolean);if(Array.isArray(d)&&d.length===0){var h,y=(h=t.current)===null||h===void 0?void 0:h.getFieldsValue();return Array.isArray(y)?y:Object.keys(y).map(function(x){return y[x]})}return(i=t.current)===null||i===void 0?void 0:i.getFieldValue(d)};return(0,v.Z)((0,v.Z)({},t.current),{},{getRowData:g,getRowsData:l,setRowData:function(i,d){var h,y,x,H;if(i==null)throw new Error("rowIndex is required");var G=I(i),Be=[e.name,(h=G==null?void 0:G.toString())!==null&&h!==void 0?h:""].flat(1).filter(Boolean),Ne=((y=t.current)===null||y===void 0||(x=y.getFieldsValue)===null||x===void 0?void 0:x.call(y))||{},Oe=(0,X.Z)(Ne,Be,(0,v.Z)((0,v.Z)({},g(i)),d||{}));return(H=t.current)===null||H===void 0?void 0:H.setFieldsValue(Oe)}})}),(0,R.useEffect)(function(){!e.controlled||r.forEach(function(g,l){var o;(o=t.current)===null||o===void 0||o.setFieldsValue((0,ue.Z)({},s(g,l),g))},{})},[r,e.controlled]),(0,R.useEffect)(function(){if(e.name){var g;t.current=e==null||(g=e.editable)===null||g===void 0?void 0:g.form}},[(C=e.editable)===null||C===void 0?void 0:C.form,e.name]);var $=K||{},$e=$.record,J=$.position,Se=$.creatorButtonText,Fe=$.newRecordType,Ke=$.parentKey,we=$.style,De=(0,Y.Z)($,Te),ee=J==="top",B=(0,R.useMemo)(function(){return S&&S<=(r==null?void 0:r.length)?!1:K!==!1&&(0,f.jsx)(q,{record:(0,Q.h)($e,r==null?void 0:r.length,r)||{},position:J,parentKey:(0,Q.h)(Ke,r==null?void 0:r.length,r),newRecordType:Fe,children:(0,f.jsx)(O.Z,(0,v.Z)((0,v.Z)({type:"dashed",style:(0,v.Z)({display:"block",margin:"10px 0",width:"100%"},we),icon:(0,f.jsx)(se.Z,{})},De),{},{children:Se||D.getMessage("editableTable.action.add","\u6DFB\u52A0\u4E00\u884C\u6570\u636E")}))})},[K,S,r==null?void 0:r.length]),Ee=(0,R.useMemo)(function(){return B?ee?{components:{header:{wrapper:function(l){var o,i=l.className,d=l.children;return(0,f.jsxs)("thead",{className:i,children:[d,(0,f.jsxs)("tr",{style:{position:"relative"},children:[(0,f.jsx)("td",{colSpan:0,style:{visibility:"hidden"},children:B}),(0,f.jsx)("td",{style:{position:"absolute",left:0,width:"100%"},colSpan:(o=u.columns)===null||o===void 0?void 0:o.length,children:B})]})]})}}}}:{tableViewRender:function(l,o){var i,d;return(0,f.jsxs)(f.Fragment,{children:[(i=(d=e.tableViewRender)===null||d===void 0?void 0:d.call(e,l,o))!==null&&i!==void 0?i:o,B]})}}:{}},[ee,B]),L=(0,v.Z)({},e.editable),Ie=(0,me.J)(function(g,l){var o,i,d;if((o=e.editable)===null||o===void 0||(i=o.onValuesChange)===null||i===void 0||i.call(o,g,l),(d=e.onValuesChange)===null||d===void 0||d.call(e,l,g),e.controlled){var h;e==null||(h=e.onChange)===null||h===void 0||h.call(e,l)}});return((e==null?void 0:e.onValuesChange)||((T=e.editable)===null||T===void 0?void 0:T.onValuesChange)||e.controlled&&(e==null?void 0:e.onChange))&&(L.onValuesChange=Ie),(0,f.jsxs)(f.Fragment,{children:[(0,f.jsx)(k.Provider,{value:m,children:(0,f.jsx)(Ze.Z,(0,v.Z)((0,v.Z)((0,v.Z)({search:!1,options:!1,pagination:!1,rowKey:V,revalidateOnFocus:!1},u),Ee),{},{tableLayout:"fixed",actionRef:m,onChange:j,editable:(0,v.Z)((0,v.Z)({},L),{},{formProps:(0,v.Z)({formRef:t},L.formProps)}),dataSource:r,onDataSourceChange:function(l){if(M(l),e.name&&J==="top"){var o,i=(0,X.Z)({},[e.name].flat(1).filter(Boolean),l);(o=t.current)===null||o===void 0||o.setFieldsValue(i)}}}))}),e.name?(0,f.jsx)(fe.Z,{name:[e.name],children:function(l){var o,i,d=(0,Re.Z)(l,[e.name].flat(1)),h=d==null?void 0:d.find(function(y,x){return!(0,ge.Z)(y,a==null?void 0:a[x])});return h&&(e==null||(o=e.editable)===null||o===void 0||(i=o.onValuesChange)===null||i===void 0||i.call(o,h,d)),null}}):null]})}function _(e){return e.name?(0,f.jsx)(ie.Z.Item,(0,v.Z)((0,v.Z)({style:{maxWidth:"100%"}},e==null?void 0:e.formItemProps),{},{name:e.name,children:(0,f.jsx)(ye.gN,{shouldUpdate:!0,name:e.name,isList:!0,children:function(T,D,j){return(0,f.jsx)(p,(0,v.Z)((0,v.Z)({},e),{},{editable:(0,v.Z)((0,v.Z)({},e.editable),{},{form:j}),value:T.value||[],onChange:T.onChange}))}})})):(0,f.jsx)(p,(0,v.Z)({},e))}_.RecordCreator=q;var Ve=_,A=n(48086),xe=n(5251),je=function(C){var T=[{title:"\u914D\u7F6E\u9879",dataIndex:"key",editable:!1,width:"15%"},{title:"\u914D\u7F6E\u503C",dataIndex:"value",valueType:function(u,a){switch(u.type){case"map":return"jsonCode";case"boolean":return"switch";case"list":return"textarea";case"password":return"password";case"int":case"long":default:break}return"text"},formItemProps:function(u,a){var m=a.rowKey,t=a.key,b=a.index;if(m===void 0)return{rules:[{required:!0,message:"\u6B64\u9879\u4E3A\u5FC5\u586B\u9879"}]};var c=u.getFieldValue(m),r=[];return c!=null&&c.required&&r.push({required:c.required,message:"\u6B64\u9879\u4E3A\u5FC5\u586B\u9879"}),(c==null?void 0:c.type)==="int"&&r.push({pattern:/[1-9][0-9]*$/,message:"\u8BE5\u9879\u503C\u4E3A\u6570\u5B57"}),(c==null?void 0:c.type)==="list"&&r.push({pattern:/^([\w]+\s*,\s*)*([\w]+)$/,message:'\u8BE5\u9879\u503C\u4E3A\u5217\u8868\uFF0C\u503C\u4E4B\u95F4\u8BF7\u4EE5","\u5206\u5272'}),(c==null?void 0:c.type)==="map"&&r.push({validator:function(s,I){try{JSON.parse(I)}catch{return Promise.reject(new Error("\u8BF7\u586B\u5165\u5408\u6CD5\u7684json\u5B57\u7B26\u4E32"))}return Promise.resolve()}}),{rules:r}}},{title:"\u63CF\u8FF0",dataIndex:"desc",editable:!1}],D=(0,R.useState)(),j=W()(D,2),S=j[0],F=j[1],K=(0,R.useState)(),V=W()(K,2),w=V[0],E=V[1],P=(0,R.useRef)();return(0,R.useEffect)(function(){var Z,u;(Z=P.current)===null||Z===void 0||(u=Z.reset)===null||u===void 0||u.call(Z)},[C]),(0,f.jsx)(Ve,{rowKey:"key",columns:T,value:w,controlled:!0,onChange:function(u){if(u!==void 0){var a=N()(u),m;try{for(a.s();!(m=a.n()).done;){var t=m.value;typeof t.value=="string"&&(t.value=t.value.trim(),t.type==="list"&&t.value!=null&&(t.value=t.value.split(/\s*,\s*/)))}}catch(b){a.e(b)}finally{a.f()}E(u)}},request:C.request,postData:function(u){F(u.map(function(b){return b.key}));var a=N()(u),m;try{for(a.s();!(m=a.n()).done;){var t=m.value;t.type==="map"&&(t.value=JSON.stringify(t.value,null,4))}}catch(b){a.e(b)}finally{a.f()}return u},actionRef:P,toolBarRender:function(){return[(0,f.jsx)(O.Z,{type:"primary",onClick:re()(U().mark(function u(){var a,m,t,b,c,r;return U().wrap(function(s){for(;;)switch(s.prev=s.next){case 0:if(w!==void 0){s.next=2;break}return s.abrupt("return");case 2:a={},m=N()(w);try{for(m.s();!(t=m.n()).done;)b=t.value,b.type=="map"?a[b.key]=JSON.parse(b.value):a[b.key]=b.value}catch(I){m.e(I)}finally{m.f()}return c=A.ZP.loading("\u6B63\u5728\u4FDD\u5B58\u914D\u7F6E..."),s.prev=6,s.next=9,C.onSave(a);case 9:if(r=s.sent,c(),(r==null?void 0:r.success)!=!0){s.next=15;break}return A.ZP.success("success",5),(C==null?void 0:C.afterSave)!=null&&(C==null||C.afterSave()),s.abrupt("return");case 15:(0,xe.oc)(r),s.next=22;break;case 18:s.prev=18,s.t0=s.catch(6),c(),A.ZP.error("\u53D1\u751F\u9519\u8BEF",10);case 22:case"end":return s.stop()}},u,null,[[6,18]])})),children:"\u4FDD\u5B58"},"save"),(0,f.jsx)(O.Z,{onClick:function(){var a;(a=P.current)===null||a===void 0||a.reload()},children:"\u91CD\u7F6E"},"reset")]},recordCreatorProps:!1,editable:{type:"multiple",editableKeys:S,onValuesChange:function(u,a){E(a)},onChange:F}})},Pe=je}}]);
