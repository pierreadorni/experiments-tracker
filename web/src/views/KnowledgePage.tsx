import { Autopilot } from '../Autopilot';
import { ProjectKnowledge } from '../ProjectKnowledge';
import type { ViewProps } from './props';

export function KnowledgePage(
  props: Pick<
    ViewProps,
    | 'autopilotActive'
    | 'choose'
    | 'experiments'
    | 'projectId'
    | 'setAutopilotActive'
    | 'setBrief'
    | 'setCreating'
    | 'setReferenceId'
    | 'status'
  >,
) {
  const {
    autopilotActive,
    choose,
    experiments,
    projectId,
    setAutopilotActive,
    setBrief,
    setCreating,
    setReferenceId,
    status,
  } = props;
  return (
    <>
      <div className="page-heading">
        <h1>Project knowledge</h1>
      </div>
      <Autopilot
        key={'autopilot-' + projectId}
        onActive={setAutopilotActive}
        projectId={projectId}
        status={status}
        open={choose}
      />
      <ProjectKnowledge
        autopilotActive={autopilotActive}
        key={projectId}
        projectId={projectId}
        models={status?.models || []}
        open={choose}
        experiments={experiments}
        useIdea={(idea) => {
          setBrief(idea.text);
          setReferenceId(idea.experiment_id);
          setCreating(true);
        }}
      />
    </>
  );
}
