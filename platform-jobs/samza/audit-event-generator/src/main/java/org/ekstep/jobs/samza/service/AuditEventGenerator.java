package org.ekstep.jobs.samza.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.ekstep.graph.dac.enums.GraphDACParams;
import org.ekstep.graph.dac.enums.SystemProperties;
import org.ekstep.graph.model.node.DefinitionDTO;
import org.ekstep.graph.model.node.RelationDefinition;
import org.ekstep.jobs.samza.service.task.JobMetrics;
import org.ekstep.jobs.samza.util.JSONUtils;
import org.ekstep.jobs.samza.util.JobLogger;
import org.ekstep.learning.util.ControllerUtil;
import org.ekstep.telemetry.TelemetryGenerator;
import org.ekstep.telemetry.TelemetryParams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author gauraw
 *
 */
public class AuditEventGenerator implements ISamzaService {

	static JobLogger LOGGER = new JobLogger(AuditEventGenerator.class);
	private Config config = null;
	private static ObjectMapper mapper = new ObjectMapper();
	private SystemStream systemStream = null;
	private static List<String> systemPropsList = null;
	private ControllerUtil util = new ControllerUtil();
	static {
		systemPropsList = Stream.of(SystemProperties.values()).map(SystemProperties::name).collect(Collectors.toList());
		systemPropsList.addAll(Arrays.asList("SYS_INTERNAL_LAST_UPDATED_ON", "lastUpdatedOn", "versionKey"));
	}

	public AuditEventGenerator() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.ekstep.jobs.samza.service.ISamzaService#initialize(org.apache.samza.
	 * config.Config)
	 */
	@Override
	public void initialize(Config config) throws Exception {
		this.config = config;
		JSONUtils.loadProperties(config);
		systemStream = new SystemStream("kafka", config.get("telemetry_raw_topic"));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.ekstep.jobs.samza.service.ISamzaService#processMessage(java.util.Map,
	 * org.ekstep.jobs.samza.service.task.JobMetrics,
	 * org.apache.samza.task.MessageCollector)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void processMessage(Map<String, Object> message, JobMetrics metrics, MessageCollector collector)
			throws Exception {
		LOGGER.debug("Telemetry Audit Started.");
		try {
			Map<String, Object> auditMap = getAuditMessage(message);
			String objectType = (String) ((Map<String, Object>) auditMap.get("object")).get("type");
			if (null != objectType) {
				collector.send(new OutgoingMessageEnvelope(systemStream, auditMap));
				LOGGER.debug("Telemetry Audit Message Sent to Topic : " + config.get("telemetry_raw_topic"));
				metrics.incSuccessCounter();
			} else {
				LOGGER.info("skipped event as the objectype is not available, event =" + auditMap);
				metrics.incSkippedCounter();
			}
		} catch (Exception e) {
			metrics.incErrorCounter();
			LOGGER.error("Failed to process message", message, e);
		}
	}

	/**
	 * @param channelId
	 * @param env
	 * @return
	 */
	private static Map<String, String> getContext(String channelId, String env) {
		Map<String, String> context = new HashMap<String, String>();
		context.put(TelemetryParams.ACTOR.name(), "org.ekstep.learning.platform");
		context.put(TelemetryParams.CHANNEL.name(), channelId);
		context.put(TelemetryParams.ENV.name(), env);
		return context;
	}

	/**
	 * @param message
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> getAuditMessage(Map<String, Object> message) throws Exception {
		Map<String, Object> auditMap = null;
		String objectId = (String) message.get(GraphDACParams.nodeUniqueId.name());
		String objectType = (String) message.get(GraphDACParams.objectType.name());
		String env = (null != objectType) ? objectType.toLowerCase().replace("image", "") : "system";
		String graphId = (String) message.get(GraphDACParams.graphId.name());
		DefinitionDTO definitionNode = util.getDefinition(graphId, objectType);
		Map<String, String> inRelations = new HashMap<>();
		Map<String, String> outRelations = new HashMap<>();
		getRelationDefinitionMaps(definitionNode, inRelations, outRelations);

		String channelId = "in.ekstep";
		String channel = (String) message.get(GraphDACParams.channel.name());
		if (null != channel)
			channelId = channel;
		Map<String, Object> transactionData = (Map<String, Object>) message.get(GraphDACParams.transactionData.name());
		Map<String, Object> propertyMap = (Map<String, Object>) transactionData.get(GraphDACParams.properties.name());
		Map<String, Object> statusMap = (Map<String, Object>) propertyMap.get(GraphDACParams.status.name());
		List<Map<String, Object>> addedRelations = (List<Map<String, Object>>) transactionData
				.get(GraphDACParams.addedRelations.name());
		List<Map<String, Object>> removedRelations = (List<Map<String, Object>>) transactionData
				.get(GraphDACParams.removedRelations.name());

		String pkgVersion = "";
		Map<String, Object> pkgVerMap = (Map<String, Object>) propertyMap.get("pkgVersion");
		if (null != pkgVerMap)
			pkgVersion = (String) pkgVerMap.get("nv");

		String prevStatus = "";
		String currStatus = "";
		if (null != statusMap) {
			prevStatus = (String) statusMap.get("ov");
			currStatus = (String) statusMap.get("nv");
		}
		List<String> props = propertyMap.keySet().stream().collect(Collectors.toList());
		props.addAll(getRelationProps(addedRelations, inRelations, outRelations));
		props.addAll(getRelationProps(removedRelations, inRelations, outRelations));
		List<String> propsExceptSystemProps = props.stream().filter(prop -> !systemPropsList.contains(prop))
				.collect(Collectors.toList());
		List<Map<String, Object>> cdata = getCData(addedRelations, removedRelations, propertyMap);
		Map<String, String> context = getContext(channelId, env);
		context.put("objectId", objectId);
		context.put(GraphDACParams.objectType.name(), objectType);
		if (StringUtils.isNotBlank(pkgVersion))
			context.put("pkgVersion", pkgVersion);
		String auditMessage = TelemetryGenerator.audit(context, propsExceptSystemProps, currStatus, prevStatus, cdata);
		LOGGER.debug("Audit Message : " + auditMessage);
		auditMap = mapper.readValue(auditMessage, new TypeReference<Map<String, Object>>() {
		});

		return auditMap;
	}

	/**
	 * @param addedRelations
	 * @param removedRelations
	 * @param propertyMap
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getCData(List<Map<String, Object>> addedRelations,
			List<Map<String, Object>> removedRelations, Map<String, Object> propertyMap) {

		List<Map<String, Object>> cdata = new ArrayList<Map<String, Object>>();

		if (null != propertyMap && !propertyMap.isEmpty() && propertyMap.containsKey("dialcodes")) {
			Map<String, Object> dialcodeMap = (Map<String, Object>) propertyMap.get("dialcodes");
			List<String> dialcodes = (List<String>) dialcodeMap.get("nv");
			if (null != dialcodes) {
				HashMap<String, Object> map = new HashMap<String, Object>();
				map.put("id", dialcodes);
				map.put("type", "DialCode");
				cdata.add(map);
			}
		}

		if (null != addedRelations && !addedRelations.isEmpty())
			prepareCData(cdata, addedRelations);

		if (null != removedRelations && !removedRelations.isEmpty())
			prepareCData(cdata, addedRelations);

		return cdata;
	}

	/**
	 * @param cdata
	 * @param relations
	 */
	private void prepareCData(List<Map<String, Object>> cdata, List<Map<String, Object>> relations) {
		for (Map<String, Object> relation : relations) {
			HashMap<String, Object> cMap = new HashMap<String, Object>();
			cMap.put("id", relation.get("id"));
			cMap.put("type", relation.get("type"));
			cdata.add(cMap);
		}
	}

	/**
	 * @param props
	 * @param addedRelations
	 * @param inRelations
	 * @param outRelations
	 */
	private List<String> getRelationProps(List<Map<String, Object>> relations, Map<String, String> inRelations,
			Map<String, String> outRelations) {
		List<String> props = new ArrayList<>();
		if (null != relations && !relations.isEmpty()) {
			for (Map<String, Object> relation : relations) {
				String key = (String) relation.get("rel") + (String) relation.get("type");
				if (StringUtils.equalsIgnoreCase((String) relation.get("dir"), "IN")) {
					props.add(inRelations.get(key + "in"));
				} else {
					props.add(outRelations.get(key + "out"));
				}
			}
		}
		return props;

	}

	/**
	 * @param definition
	 * @param inRelations
	 * @param outRelations
	 */
	private void getRelationDefinitionMaps(DefinitionDTO definition, Map<String, String> inRelations,
			Map<String, String> outRelations) {
		if (null != definition) {
			if (null != definition.getInRelations() && !definition.getInRelations().isEmpty()) {
				for (RelationDefinition rDef : definition.getInRelations()) {
					getRelationDefinitionKey(rDef, inRelations, "in");
				}
			}
			if (null != definition.getOutRelations() && !definition.getOutRelations().isEmpty()) {
				for (RelationDefinition rDef : definition.getOutRelations()) {
					getRelationDefinitionKey(rDef, outRelations, "out");
				}
			}
		}
	}

	/**
	 * @param rDef
	 * @param relDefMap
	 * @param rel
	 */
	private static void getRelationDefinitionKey(RelationDefinition rDef, Map<String, String> relDefMap, String rel) {
		if (null != rDef.getObjectTypes() && !rDef.getObjectTypes().isEmpty()) {
			for (String type : rDef.getObjectTypes()) {
				String key = rDef.getRelationName() + type + rel;
				relDefMap.put(key, rDef.getTitle());
			}
		}
	}

}