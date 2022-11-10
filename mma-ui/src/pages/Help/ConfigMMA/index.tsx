import {PageContainer} from "@ant-design/pro-components";
import {Steps, Typography, Image} from "antd";
import mmaConfigImage from "./mma-config.jpg";

const {Title, Paragraph, Text, Link} = Typography;
const { Step } = Steps;


export default () => {
    return (
        <PageContainer>
            <Typography>
                <Title level={2}>安装与配置</Title>
                <Paragraph code={true}>
                    蚂蚁的企业级产品是一个庞大且复杂的体系。这类产品不仅量级巨大且功能复杂，而且变动和并发频繁，常常需要设计与开发能够快速的做出响应。同时这类产品中有存在很多类似的页面以及组件，可以通过抽象得到一些稳定且高复用性的内容。
                    <Image
                        src={mmaConfigImage}
                    />

                    <pre>
                        <code>
                            [mysql]<br/>
                            host = 127.0.0.1<br/>
                            port = 3306<br/>
                            db = mma2<br/>
                            username = root<br/>
                            password = example<br/>
                            <br/>
                            [mma]<br/>
                            listening_port = 6060<br/>
                        </code>
                    </pre>
                </Paragraph>
                <Paragraph>
                    随着商业化的趋势，越来越多的企业级产品对更好的用户体验有了进一步的要求。带着这样的一个终极目标，我们（蚂蚁金服体验技术部）经过大量的项目实践和总结，逐步打磨出一个服务于企业级产品的设计体系
                    Ant Design。基于<Text mark>『确定』和『自然』</Text>
                    的设计价值观，通过模块化的解决方案，降低冗余的生产成本，让设计者专注于
                    <Text strong>更好的用户体验</Text>。
                </Paragraph>
                <Title level={2}>设计资源</Title>
                <Paragraph>
                    我们提供完善的设计原则、最佳实践和设计资源文件（<Text code>Sketch</Text> 和
                    <Text code>Axure</Text>），来帮助业务快速设计出高质量的产品原型。
                </Paragraph>

                <Paragraph>
                    <ul>
                        <li>
                            <Link href="/docs/spec/proximity-cn">设计原则</Link>
                        </li>
                        <li>
                            <Link href="/docs/spec/overview-cn">设计模式</Link>
                        </li>
                        <li>
                            <Link href="/docs/resources-cn">设计资源</Link>
                        </li>
                    </ul>
                </Paragraph>

                <Paragraph>
                    按<Text keyboard>Esc</Text>键退出阅读……
                </Paragraph>
            </Typography>
        </PageContainer>
    )
}