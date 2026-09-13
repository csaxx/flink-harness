package org.flink.harness.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.flink.harness.graph.function.AbstractFunctionHarness;
import org.flink.harness.graph.function.rich.KeyedProcessFunctionHarness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The validated, immutable job-graph layout of a workflow: nodes, edges, source/sink ids and
 * the {@link TypeInformation} hints supplied at registration. Mirrors Flink's
 * {@code StreamGraph} (typed user topology), not its {@code JobGraph} (parallelized deployment
 * descriptor).
 *
 * <p>Construction via {@link #create} runs all structural validation (topology, edge types,
 * keyed-edge requirements), so an instance is always valid; {@code WorkflowBuilder} remains the
 * only intended entry point. Everything structural is derived once here — the outbound-edge
 * index, the keyed-harness view and the serializable {@link WorkflowNode} projection — so
 * consumers never re-derive graph shape.
 *
 * <p>Not {@code Serializable}: nodes are live harnesses. The {@link #workflowNodes()} list is
 * the serializable view intended for storage, transport (e.g. REST) and visualization.
 */
public final class WorkflowStreamGraph {

    private final Map<String, StreamNode> nodes;
    private final List<StreamEdge> edges;
    private final Set<String> sourceIds;
    private final Set<String> sinkIds;
    private final Map<String, TypeInformation<?>> inputTypes;
    private final Map<String, TypeInformation<?>> outputTypes;
    private final Map<String, List<StreamEdge>> outboundEdges;
    private final List<KeyedProcessFunctionHarness> keyedHarnesses;
    private final List<WorkflowNode> workflowNodes;

    private WorkflowStreamGraph(
            Map<String, StreamNode> nodes,
            List<StreamEdge> edges,
            Set<String> sourceIds,
            Set<String> sinkIds,
            Map<String, TypeInformation<?>> inputTypes,
            Map<String, TypeInformation<?>> outputTypes) {
        this.nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
        this.edges = List.copyOf(edges);
        this.sourceIds = Collections.unmodifiableSet(new LinkedHashSet<>(sourceIds));
        this.sinkIds = Collections.unmodifiableSet(new LinkedHashSet<>(sinkIds));
        this.inputTypes = Collections.unmodifiableMap(new LinkedHashMap<>(inputTypes));
        this.outputTypes = Collections.unmodifiableMap(new LinkedHashMap<>(outputTypes));
        this.outboundEdges = indexOutboundEdges(edges);
        this.keyedHarnesses = collectKeyedHarnesses(this.nodes);
        this.workflowNodes = deriveWorkflowNodes();
    }

    /** Validates the raw builder data, then freezes it. Validation order is observable and must
     * match the historical {@code WorkflowBuilder.build()} order: topology first, then per-edge
     * (unknown endpoint → side/main type check → keyed-edge requirement). */
    public static WorkflowStreamGraph create(
            Map<String, StreamNode> nodes,
            List<StreamEdge> edges,
            Set<String> sourceIds,
            Set<String> sinkIds,
            Map<String, TypeInformation<?>> inputTypes,
            Map<String, TypeInformation<?>> outputTypes,
            boolean optOutTypeValidation) {
        validateTopology(edges, sourceIds, sinkIds);
        validateEdges(nodes, edges, inputTypes, outputTypes, optOutTypeValidation);
        return new WorkflowStreamGraph(nodes, edges, sourceIds, sinkIds, inputTypes, outputTypes);
    }

    // --------------------------------------------------------------------------------------------
    // validation (moved verbatim from WorkflowBuilder)
    // --------------------------------------------------------------------------------------------

    /** Sources are roots and sinks are leaves; only functions may sit in between. */
    private static void validateTopology(
            List<StreamEdge> edges, Set<String> sourceIds, Set<String> sinkIds) {
        for (StreamEdge edge : edges) {
            if (sourceIds.contains(edge.dst())) {
                throw new IllegalStateException(
                        "source node " + edge.dst() + " must not receive inbound edges (edge from " + edge.src() + ")");
            }
            if (sinkIds.contains(edge.src())) {
                throw new IllegalStateException(
                        "sink node " + edge.src() + " must not have outbound edges (edge to " + edge.dst() + ")");
            }
        }
    }

    private static void validateEdges(
            Map<String, StreamNode> nodes,
            List<StreamEdge> edges,
            Map<String, TypeInformation<?>> inputTypes,
            Map<String, TypeInformation<?>> outputTypes,
            boolean optOutTypeValidation) {
        for (StreamEdge edge : edges) {
            requireNode(nodes, edge.src());
            StreamNode dstNode = requireNode(nodes, edge.dst());

            TypeInformation<?> srcOut = outputTypes.get(edge.src());
            TypeInformation<?> dstIn = inputTypes.get(edge.dst());

            // side channel: the tag's declared type must match the destination input type
            if (edge.sideChannel()) {
                TypeInformation<?> tagType = edge.sideTag().getTypeInfo();
                if (tagType != null) {
                    if (dstIn != null && !tagType.equals(dstIn)) {
                        throw new IllegalStateException(
                                "side-output edge type mismatch: " + edge.src() + "#" + edge.sideTag()
                                        + " outputs " + tagType + " but " + edge.dst() + " expects " + dstIn);
                    }
                } else if (!optOutTypeValidation) {
                    throw new IllegalStateException(
                            "unresolved side-output tag type for " + edge.src() + "\u2192" + edge.dst()
                                    + " — provide TypeInformation hints or opt out explicitly");
                }
            } else {
                // main channel: known endpoint types must be equal; either side unknown = unresolved
                boolean known = srcOut != null && dstIn != null;
                if (known && !srcOut.equals(dstIn)) {
                    throw new IllegalStateException(
                            "edge type mismatch: " + edge.src() + " outputs " + srcOut
                                    + " but " + edge.dst() + " expects " + dstIn);
                }
                if (!known && !optOutTypeValidation) {
                    throw new IllegalStateException(
                            "unresolved edge type for " + edge.src() + "\u2192" + edge.dst()
                                    + " — provide TypeInformation hints at registration or opt out explicitly");
                }
            }

            // a keyed function is meaningless without a key selector on every inbound edge
            if (dstNode.requiresKeyedEdge() && !edge.keyed()) {
                throw new IllegalStateException(
                        "KeyedProcessFunction " + edge.dst() + " received unkeyed edge from " + edge.src());
            }
        }
    }

    private static StreamNode requireNode(Map<String, StreamNode> nodes, String id) {
        StreamNode node = nodes.get(id);
        if (node == null) {
            throw new IllegalStateException("unknown node id: " + id);
        }
        return node;
    }

    // --------------------------------------------------------------------------------------------
    // derived structure
    // --------------------------------------------------------------------------------------------

    private static Map<String, List<StreamEdge>> indexOutboundEdges(List<StreamEdge> edges) {
        Map<String, List<StreamEdge>> index = new LinkedHashMap<>();
        for (StreamEdge edge : edges) {
            index.computeIfAbsent(edge.src(), srcId -> new ArrayList<>()).add(edge);
        }
        Map<String, List<StreamEdge>> frozen = new LinkedHashMap<>();
        index.forEach((srcId, edgeList) -> frozen.put(srcId, List.copyOf(edgeList)));
        return Collections.unmodifiableMap(frozen);
    }

    private static List<KeyedProcessFunctionHarness> collectKeyedHarnesses(Map<String, StreamNode> nodes) {
        List<KeyedProcessFunctionHarness> keyed = new ArrayList<>();
        for (StreamNode node : nodes.values()) {
            if (node instanceof KeyedProcessFunctionHarness keyedHarness) {
                keyed.add(keyedHarness);
            }
        }
        return List.copyOf(keyed);
    }

    /** The serializable projection: kinds derive from the source/sink id sets, type names from
     * the {@link TypeInformation} hints ({@link WorkflowNode#UNKNOWN_TYPE} when absent). */
    private List<WorkflowNode> deriveWorkflowNodes() {
        Map<String, List<String>> successors = new LinkedHashMap<>();
        for (StreamEdge edge : edges) {
            successors.computeIfAbsent(edge.src(), srcId -> new ArrayList<>()).add(edge.dst());
        }
        List<WorkflowNode> projection = new ArrayList<>();
        for (String id : nodes.keySet()) {
            WorkflowNode.Kind kind = sourceIds.contains(id)
                    ? WorkflowNode.Kind.SOURCE
                    : sinkIds.contains(id) ? WorkflowNode.Kind.SINK : WorkflowNode.Kind.FUNCTION;
            projection.add(new WorkflowNode(id, kind,
                    typeName(inputTypes.get(id)), typeName(outputTypes.get(id)),
                    List.copyOf(successors.getOrDefault(id, List.of()))));
        }
        return List.copyOf(projection);
    }

    private static String typeName(TypeInformation<?> typeInfo) {
        return typeInfo == null ? WorkflowNode.UNKNOWN_TYPE : typeInfo.toString();
    }

    // --------------------------------------------------------------------------------------------
    // accessors
    // --------------------------------------------------------------------------------------------

    /** All nodes keyed by id, in registration order (drives {@code workflowNodes()} order and
     * gauge last-wins aggregation). */
    public Map<String, StreamNode> nodes() {
        return nodes;
    }

    /** Node lookup with the canonical unknown-id failure; prefer over {@code nodes().get()}. */
    public StreamNode node(String id) {
        StreamNode node = nodes.get(id);
        if (node == null) {
            throw new IllegalStateException("unknown node id: " + id);
        }
        return node;
    }

    /** The wrapped Flink function for function nodes, or the node itself for synthetic
     * source/sink nodes. */
    public Object unwrapped(String id) {
        StreamNode node = node(id);
        if (node instanceof AbstractFunctionHarness<?> functionHarness) {
            return functionHarness.getFunction();
        }
        return node;
    }

    public List<StreamEdge> edges() {
        return edges;
    }

    /** Outbound edges of a node in registration order; empty when the node is a leaf. */
    public List<StreamEdge> outboundEdgesOf(String srcId) {
        return outboundEdges.getOrDefault(srcId, List.of());
    }

    public Set<String> sourceIds() {
        return sourceIds;
    }

    public Set<String> sinkIds() {
        return sinkIds;
    }

    /** Input-type hints keyed by node id; only ids registered with a hint appear. */
    public Map<String, TypeInformation<?>> inputTypes() {
        return inputTypes;
    }

    /** Output-type hints keyed by node id; only ids registered with a hint appear. */
    public Map<String, TypeInformation<?>> outputTypes() {
        return outputTypes;
    }

    /** Keyed harnesses in registration order — the workflow's timer-firing surface. */
    public List<KeyedProcessFunctionHarness> keyedHarnesses() {
        return keyedHarnesses;
    }

    /** The serializable introspection view of this graph (storage/transport/visualization). */
    public List<WorkflowNode> workflowNodes() {
        return workflowNodes;
    }

    // --------------------------------------------------------------------------------------------
    // lifecycle
    // --------------------------------------------------------------------------------------------

    /** Opens every node in registration order for {@code initializeAtBuild()}. Called by the
     * builder right after construction, before any workflow (and its lock) exists — hence
     * unlocked, unlike {@code StandaloneWorkflow.close()}. */
    public void openAll() {
        for (StreamNode node : nodes.values()) {
            node.open();
        }
    }
}
