import * as express from "express";

const users = [
  { id:"1", name: 'Umi', nickName: 'U', gender: 'MALE' },
  { id:"2", name: 'Fish', nickName: 'B', gender: 'FEMALE' },
  { id:"3", name: 'Big', nickName: 'Ca', gender: 'FEMALE' },
];

let id=4;

export default {
  'GET /api/v1/queryUserList': (req: any, res: any) => {
    res.json({
      success: true,
      data: { list: users },
      errorCode: 0,
    });
  },
  'POST /api/v1/user/': (req: express.Request, res: any) => {
    let body: API.UserInfoVO = req.body as API.UserInfoVO;
    users.push({
      id: id.toString(),
      name:  body.name || "a",
      nickName:  body.nickName || "b",
      gender: "mail"
    })

    id += 1;

    res.json({
      success: true,
      errorCode: 0,
    });
  },
  'PUT /api/v1/user/': (req: any, res: any) => {
    res.json({
      success: true,
      errorCode: 0,
    });
  },
};
