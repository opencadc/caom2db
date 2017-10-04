package esac.archive.ehst.dl.caom2.artifac.sync.checksums.db;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jduran
 *
 */
public class ConfigProperties
{

    private static boolean initialized = false;
    private static Properties prop;
    private static String pathToConfigFile = "";
    private static final Logger logger = Logger.getLogger(ConfigProperties.class.getName());

    private static ConfigProperties instance = null;

    public static ConfigProperties getInstance()
    {
        if (!initialized)
        {
            logger.log(Level.SEVERE, "Error: Init() method must be called first.");
            System.exit(1);
        }

        if (instance == null)
        {
            instance = new ConfigProperties();
        }
        return instance;
    }

    public static void Init(String path)
    {
        logger.log(Level.INFO, "Creating properties file");
        prop = new Properties();
        pathToConfigFile = path;
        initialized = true;
    }

    private ConfigProperties()
    {
        try
        {
            logger.log(Level.INFO, "props " + prop);
            InputStream stream = getClass().getClassLoader().getResourceAsStream(pathToConfigFile);
            logger.log(Level.INFO, "Loading properties file '" + pathToConfigFile + "': " + stream);
            if (stream != null)
            {
                prop.load(stream);
            }
            else
            {
                throw new FileNotFoundException("property file " + pathToConfigFile + " not found in the classpath");
            }
            logger.log(Level.INFO, "Properties file '" + pathToConfigFile + "' loaded");
        }
        catch (IOException e)
        {
            logger.log(Level.SEVERE, "Error loading properties file '" + pathToConfigFile + "'");
            e.printStackTrace();
        }

    }

    public String getProperty(String property)
    {
        String result = null;
        result = prop.getProperty(property);
        if (result == null || result.equals(""))
        {
            logger.log(Level.SEVERE, "Error reading properties file '" + pathToConfigFile + "'. There should be a parameter named " + property);
        }
        return result;
    }

    public void setProperty(String property, String value)
    {
        prop.setProperty(property, value);
    }

}