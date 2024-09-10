import {useEffect, useState} from "react";
import {getLoadingMetaProgress, updateSource} from "@/services/source";
import {Alert, Modal, Progress} from "antd";
import {useIntl} from "umi";
import {fm} from "@/components/i18n";

export const SourceLoader = (
    {dataSource, visible, setVisible, loaderId}:
        { dataSource: API.DataSource | undefined, visible: boolean, setVisible: (_: boolean) => void, loaderId: number}
) => {

    const [progress, setProgress] = useState<number>(0);
    const [error, setError] = useState<String>("");
    const [closeable, setCloseable] = useState(false);
    const intl = useIntl();

    let sourceId = dataSource?.id;

    const handleCancel = () => {
        setVisible(false);
        setCloseable(false);
    };

    useEffect(() => {
        setError("");
        setProgress(0);

        if (sourceId == undefined) {
            return;
        }

        setCloseable(false);

        updateSource(sourceId).then(async () => {
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
                        }
                    } else {
                        setError(res.message);
                        setCloseable(true);
                        clearInterval(sId);
                    }
                }, 2000)
            })
            .catch((e) => {
                let res = e.response.data;
                setCloseable(true);
                setError(res.message);
            })
    }, [loaderId]);

    return (
        <Modal
            title={`"${dataSource?.name}" ${fm(intl, "pages.Source.SourceList.components.SourceLoader.progress", "元数据更新进度")}`}
            open={visible}
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