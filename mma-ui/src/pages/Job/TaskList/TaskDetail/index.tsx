import {PageContainer} from "@ant-design/pro-components";

export default (props: {task: API.Task}) => {
    return (
        <PageContainer>
            task详情: {JSON.stringify(props.task, null, 4)}
        </PageContainer>
    )
}