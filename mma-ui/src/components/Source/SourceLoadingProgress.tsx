import {useEffect, useState} from "react";
import {getLoadingMetaProgress, updateSource} from "@/services/source";
import {Alert, Modal, Progress} from "antd";
import {history} from "@@/core/history";
import {SOURCES_ROUTE} from "@/constant";

export const SourceLoadingProgress = (props: { dataSource: API.DataSource | undefined, visible: boolean, onCancel: () => void}) => {
    let ds = props?.dataSource;

    const [progress, setProgress] = useState<number>(0);
    const [error, setError] = useState<String>("");
    const [closeable, setCloseable] = useState(false);
    let sourceId = ds?.id;

    const handleCancel = () => {
        props.onCancel();
        setCloseable(false);
    };

    useEffect(() => {
        if (sourceId == undefined) {
            return;
        }

        setCloseable(false);
        setError("");
        setProgress(0);
        let sId = setInterval(async () => {
            let res = await getLoadingMetaProgress(sourceId as number);
            if (res.success) {
                let progressNum = res["progress"];

                if (progressNum > 0) {
                    setProgress(progressNum);
                }

                if (progressNum >= 100 || progressNum < 0) {
                    clearInterval(sId);
                    setCloseable(true);
                    setProgress(100);
                    history.push(SOURCES_ROUTE);
                }
            } else {
                setError(res.message);
                setCloseable(true);
                clearInterval(sId);
            }
        }, 2000);
    }, [sourceId]);

    return (
        <Modal
            title={`"${ds?.name}"元数据更新进度`}
            visible={props.visible}
            closable={closeable}
            keyboard={false}
            maskClosable={false}
            destroyOnClose={true}
            footer={null}
            onCancel={handleCancel}
        >
            { error === "" ? "" : <Alert message={error} type="error" showIcon /> }
            <Progress percent={progress} status={error === ""? "active": "exception"} />
        </Modal>
    );
}