// 示例方法，没有实际意义
import {message} from "antd";

export function paramsSerializer(params: any): string {
  let kvList = [];

  for (let key in params) {
    let value = params[key];

    if (value === undefined) {
      continue;
    }

    if (Array.isArray(value)) {
      for (let v of value) {
        kvList.push(`${key}=${encodeURI(v)}`)
      }

      continue;
    }

    kvList.push(`${key}=${encodeURI(value)}`);
  }

  return kvList.join("&");
}

export function formatSize(size: number): string {
  const kb = 1024;
  const mb = 1024 * kb;
  const gb = 1024 * mb;
  const tb = 1024 * gb;
  const pb = 1024 * tb;

  const units = [kb, mb, gb, tb, pb];
  const unitNames = ["bytes", "KB", "MB", "GB", "TB"];

  for (let i in units) {
    let unit = units[i];

    if (size < unit) {
      return `${(size / (unit / 1024)).toFixed(2)} ${unitNames[i]}`;
    }
  }

  return `${(size / pb).toFixed(2)} PB`;
}


export function uuidV4() : string {
  return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, c =>
      //@ts-ignore
      (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)
  );
}

export function nowTime(): string {
  let d = new Date();

  //2022-08-23/10:38:59
  return `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()}/${d.getHours()}:${d.getMinutes()}:${d.getSeconds()}`;
}

export function showErrorMsg(res: API.MMARes<any>) {
  if (res?.errors != undefined) {
    const errors = res.errors;

    for (const key in errors) {
      const error = errors[key]
      message.error(key + " " + error, 10);
    }
  } else {
    message.error(res.message, 10);
  }
}