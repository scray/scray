package scray.common.properties;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import scray.common.properties.ScrayProperties.Phase;

public class ScrayPropertyRegistration {

	/*
	 * Modules can implement these interfaces to register their own properties
	 */

	public static interface PropertyRegistrator {
		public void register();
	}

	public static interface PropertyLoader {
		public void load();

		public String getId();
	}

	public static abstract class PropertyLoaderImpl implements PropertyLoader {

		protected String id;

		public String getId() {
			return id;
		}

		public PropertyLoaderImpl(String id) {
			this.id = id;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PropertyLoaderImpl other = (PropertyLoaderImpl) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			return true;
		}
	}

	private static Set<PropertyRegistrator> registrators = new HashSet<PropertyRegistrator>();
	private static List<PropertyLoader> loaders = new ArrayList<PropertyLoader>();

	/**
	 * registers registrars which register and load properties
	 * 
	 * @param registrator
	 */
	public static synchronized void addRegistrar(PropertyRegistrator registrator) {
		registrators.add(registrator);
	}

	/**
	 * registers registrars which register and load properties
	 * 
	 * @param registrator
	 */
	public static synchronized void addLoader(PropertyLoader loader) {
		if (!loaders.contains(loader)) {
			loaders.add(loader);
		}
	}

	/**
	 * Performs the property phase changes. May only be called once.
	 * 
	 * @throws PropertyException
	 */
	public static synchronized void performPropertySetup()
			throws PropertyException {

		if (ScrayProperties.getPhase().equals(ScrayProperties.Phase.register)) {

			for (PropertyRegistrator reg : registrators) {
				reg.register();
			}

			ScrayProperties.setPhase(Phase.config);

			for (PropertyLoader pl : loaders) {
				pl.load();
			}

			ScrayProperties.setPhase(Phase.use);
		}

	}

}
