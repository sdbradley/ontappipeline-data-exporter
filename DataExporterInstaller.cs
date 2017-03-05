using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;


namespace OTP.DataExporter {
    [RunInstaller(true)]
    public partial class DataExporterInstaller : System.Configuration.Install.Installer {
        public DataExporterInstaller() {
            InitializeComponent();
        }
    }
}
