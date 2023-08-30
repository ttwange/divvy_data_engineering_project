from prefect.deployments import Deployment 
from prefect.infrastructure.container import DockerContainer
from D_param import etl_parent_flow

docker_block = DockerContainer.load("divvy")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='ddocker-flow',
    infrastructure=docker_block
)
    
if __name__ == "__main__":
    docker_dep.apply()