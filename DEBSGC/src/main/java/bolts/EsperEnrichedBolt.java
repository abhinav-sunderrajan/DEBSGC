package bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;
import beans.SmartPlugBean;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

/**
 * A special bolt which makes use of the awesome constructs provided by Esper
 * CEP.
 * 
 * @author abhinav
 * 
 */
public abstract class EsperEnrichedBolt implements IRichBolt {

	/**
	 * 
	 */
	protected static final long serialVersionUID = 1L;
	protected EPServiceProvider cep;
	protected EPAdministrator cepAdm;
	protected Configuration cepConfig;
	protected EPRuntime cepRT;
	protected OutputCollector _collector;
	protected String[] eventTypes;
	protected String[] queries;
	private String esperEngineName;

	/**
	 * Initialize with the event types i.e. POJOs the Esper component will be
	 * handling and the EPL queries.
	 * 
	 * @param eventTypes
	 *            The fully classified names of the POJO classes using the
	 *            class.getName() method.
	 * @param queries
	 *            The EPL queries.
	 * @param esperEngineName
	 *            A unique name for the Esper engine instance.
	 */
	public EsperEnrichedBolt(String[] eventTypes, String[] queries, String esperEngineName) {

		this.eventTypes = eventTypes;
		this.queries = queries;
		this.esperEngineName = esperEngineName;

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		_collector = collector;
		cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(false);
		cep = EPServiceProviderManager.getProvider(esperEngineName, cepConfig);

		for (int i = 0; i < eventTypes.length; i++) {
			String[] split = eventTypes[i].split("\\.");
			cepConfig.addEventType(split[split.length - 1], eventTypes[i]);
		}
		EPStatement cepStatement = null;
		cepRT = cep.getEPRuntime();
		cepAdm = cep.getEPAdministrator();
		for (int i = 0; i < queries.length; i++) {
			if (i == queries.length - 1) {
				cepStatement = cepAdm.createEPL(queries[i]);
			} else {
				cepAdm.createEPL(queries[i]);
			}

		}

		cepStatement.setSubscriber(this);

	}

	@Override
	public void execute(Tuple input) {
		cepRT.sendEvent((SmartPlugBean) input.getValue(0));

	}

}
