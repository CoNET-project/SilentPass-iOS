# Uncomment the next line to define a global platform for your project
# platform :ios, '9.0'

post_install do |installer|
  installer.generated_projects.each do |project|
    project.targets.each do |target|
      target.build_configurations.each do |config|
            config.build_settings['IPHONEOS_DEPLOYMENT_TARGET'] = '13.0'
            config.build_settings['GENERATE_INFOPLIST_FILE'] = "NO"
       end
    end
  end
end

platform :ios, '13.0'

target 'CoNETVPN1' do
  # Comment the next line if you don't want to use dynamic frameworks
  use_frameworks!

#pod 'BlueSocket'
#pod 'SwiftSocket'
pod 'SVProgressHUD'
pod "PromiseKit"
pod 'SwiftyStoreKit'
pod 'Firebase/Analytics'
pod 'GCDWebServer'



#pod "ObjectivePGP"
#
#pod "web3swift"


  # Pods for CoNETVPN1

  target 'CoNETVPN1Tests' do
    inherit! :search_paths
    # Pods for testing
  end

  target 'CoNETVPN1UITests' do
    # Pods for testing
  end

end

target 'VPN' do
  # Comment the next line if you don't want to use dynamic frameworks
  use_frameworks!

  pod "PromiseKit"
#  pod 'web3swift'

#  pod 'Starscream'
#  pod "PromiseKit"
  # Pods for VPN

end
