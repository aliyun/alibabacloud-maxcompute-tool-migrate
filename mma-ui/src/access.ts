/**
 * @see https://umijs.org/zh-CN/plugins/plugin-access
 * */
export default function access(initialState: { inited: boolean }) {
  return {
    canUse: initialState.inited
  }
}
