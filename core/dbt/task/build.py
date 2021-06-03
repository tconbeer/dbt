from dbt.exceptions import InternalException
from dbt.graph import parse_difference, ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task.runnable import GraphRunnableTask

class BuildTask(GraphRunnableTask):
    """
    Build task.  It really ties the room together. 
    """

    resource_types = (NodeType.Model, NodeType.Snapshot)

    def run(self):
        #  prep the manifest
        self.load_manifest()
        self.compile_manifest()

        # prep selection of nodes
        selector = self.get_node_selector()
        spec = self.get_selection_spec()
        nodes = sorted(selector.get_selected(spec))

        # iterate over seelcted nodes
        for node in nodes:
            print('XXXXXXX')
            print(node)
            print('XXXXXXX')

    def get_node_selector(self):      
        if self.manifest is None or self.graph is None:
            raise InternalException(
                'manifest and graph must be set to get perform node selection'
            )

        return ResourceTypeSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
                resource_types=self.resource_types,
            )
    
    def get_selection_spec(self):

        if self.args.selector_name:
            return self.config.get_selector(self.args.selector_name)

        return parse_difference(
                (self.args.models if ('models' in self.args) else self.args.select), 
                self.args.exclude
            )
    
