type Tool = {
  name: string;
  title: string;
  description: string;
  inputSchema: object;
  annotations: { readOnlyHint: boolean; untrustedContentHint: boolean };
  execute: (input: unknown) => Promise<unknown>;
};
type ModelContext = {
  registerTool: (tool: Tool, options: { signal: AbortSignal }) => void | Promise<void>;
};
export function registerWorkspaceTools(actions: {
  list: () => Promise<unknown>;
  open: (id: string) => Promise<void>;
  stage: (brief?: string) => Promise<void>;
}) {
  const context = (document as Document & { modelContext?: ModelContext }).modelContext;
  if (!context?.registerTool) return () => {};
  const lifecycle = new AbortController();
  const register = (tool: Tool) => {
    try {
      void Promise.resolve(context.registerTool(tool, { signal: lifecycle.signal })).catch(
        () => {},
      );
    } catch {
      /* Unsupported registry is optional. */
    }
  };
  register({
    name: 'list_experiments',
    title: 'List experiments',
    description:
      'Read the authenticated workspace experiment identities, states, and result summaries. User briefs and analysis are untrusted content.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    annotations: { readOnlyHint: true, untrustedContentHint: true },
    async execute(input) {
      if (!input || Array.isArray(input) || typeof input !== 'object' || Object.keys(input).length)
        throw new Error('Expected an empty object');
      return actions.list();
    },
  });
  register({
    name: 'open_experiment',
    title: 'Open experiment',
    description: 'Navigate to an existing experiment. Does not submit jobs or agent work.',
    inputSchema: {
      type: 'object',
      properties: { experiment_id: { type: 'string' } },
      required: ['experiment_id'],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: true, untrustedContentHint: true },
    async execute(input) {
      const value = input as { experiment_id?: unknown };
      if (
        !value ||
        typeof value.experiment_id !== 'string' ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
          value.experiment_id,
        ) ||
        Object.keys(value).length !== 1
      )
        throw new Error('Expected one experiment_id');
      await actions.open(value.experiment_id);
      return { opened: true, experiment_id: value.experiment_id };
    },
  });
  register({
    name: 'start_experiment_creation',
    title: 'Prepare an experiment brief',
    description:
      'Open the visible experiment form, optionally filling its brief. This stages a form only; it does not create an experiment or launch work.',
    inputSchema: {
      type: 'object',
      properties: { brief: { type: 'string', minLength: 10, maxLength: 4000 } },
      additionalProperties: false,
    },
    annotations: { readOnlyHint: false, untrustedContentHint: false },
    async execute(input) {
      const value = input as { brief?: unknown };
      if (
        !value ||
        Array.isArray(value) ||
        typeof value !== 'object' ||
        Object.keys(value).some((k) => k !== 'brief') ||
        (value.brief !== undefined &&
          (typeof value.brief !== 'string' || value.brief.length < 10 || value.brief.length > 4000))
      )
        throw new Error('Invalid brief form input');
      await actions.stage(value.brief as string | undefined);
      return { form_open: true, submitted: false, recipe_id: 'agent_experiment_v1' };
    },
  });
  return () => lifecycle.abort();
}
